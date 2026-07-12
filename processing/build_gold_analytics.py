from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum as spark_sum,
    avg,
    round,
    to_date,
    date_format,
    datediff,
)

BRONZE_BASE = "hdfs://namenode:9000/olist/bronze"
GOLD_BASE = "hdfs://namenode:9000/olist/gold"


def build_spark():
    return (
        SparkSession.builder
        .appName("olist-gold-analytics")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_table(spark, name):
    return spark.read.parquet(f"{BRONZE_BASE}/{name}")


def write_gold(df, name):
    output_path = f"{GOLD_BASE}/{name}"
    (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(output_path)
    )
    print(f"OK: wrote {name} to {output_path}")


def main():
    spark = build_spark()

    orders = read_table(spark, "orders")
    order_items = read_table(spark, "order_items")
    payments = read_table(spark, "order_payments")
    customers = read_table(spark, "customers")
    products = read_table(spark, "products")

    orders_clean = (
        orders
        .withColumn("purchase_date", to_date(col("order_purchase_timestamp")))
        .withColumn("purchase_month", date_format(col("order_purchase_timestamp"), "yyyy-MM"))
        .withColumn("delivery_days", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")))
    )

    monthly_orders = (
        orders_clean
        .groupBy("purchase_month")
        .agg(count("*").alias("order_count"))
        .orderBy("purchase_month")
    )

    payment_summary = (
        payments
        .groupBy("payment_type")
        .agg(
            count("*").alias("payment_count"),
            round(spark_sum("payment_value"), 2).alias("total_payment_value"),
            round(avg("payment_value"), 2).alias("avg_payment_value"),
        )
        .orderBy(col("total_payment_value").desc())
    )

    category_sales = (
        order_items
        .join(products, "product_id", "left")
        .groupBy("product_category_name")
        .agg(
            count("*").alias("item_count"),
            round(spark_sum("price"), 2).alias("total_sales"),
            round(avg("price"), 2).alias("avg_price"),
        )
        .orderBy(col("total_sales").desc())
    )

    state_orders = (
        orders_clean
        .join(customers, "customer_id", "left")
        .groupBy("customer_state")
        .agg(count("*").alias("order_count"))
        .orderBy(col("order_count").desc())
    )

    delivery_performance = (
        orders_clean
        .filter(col("delivery_days").isNotNull())
        .groupBy("order_status")
        .agg(
            count("*").alias("order_count"),
            round(avg("delivery_days"), 2).alias("avg_delivery_days"),
        )
        .orderBy(col("order_count").desc())
    )

    write_gold(monthly_orders, "monthly_orders")
    write_gold(payment_summary, "payment_summary")
    write_gold(category_sales, "category_sales")
    write_gold(state_orders, "state_orders")
    write_gold(delivery_performance, "delivery_performance")

    spark.stop()


if __name__ == "__main__":
    main()