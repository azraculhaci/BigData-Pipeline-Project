from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, round, to_timestamp, datediff, date_format

spark = (
    SparkSession.builder
    .appName("Olist Phase 2 Simple Data Pipeline")
    .getOrCreate()
)

input_base = "hdfs://namenode:9000/data/olist/parquet"
output_base = "hdfs://namenode:9000/data/olist/processed"

orders = spark.read.parquet(f"{input_base}/olist_orders_dataset")
order_items = spark.read.parquet(f"{input_base}/olist_order_items_dataset")
payments = spark.read.parquet(f"{input_base}/olist_order_payments_dataset")
customers = spark.read.parquet(f"{input_base}/olist_customers_dataset")
products = spark.read.parquet(f"{input_base}/olist_products_dataset")

orders_clean = (
    orders
    .dropDuplicates(["order_id"])
    .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))
    .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))
    .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date")))
)

order_items_clean = order_items.dropDuplicates()
payments_clean = payments.dropDuplicates()
customers_clean = customers.dropDuplicates(["customer_id"])
products_clean = products.dropDuplicates(["product_id"])

orders_enriched = (
    orders_clean
    .join(customers_clean, "customer_id", "left")
    .join(order_items_clean, "order_id", "left")
    .join(products_clean, "product_id", "left")
    .join(payments_clean, "order_id", "left")
    .withColumn(
        "delivery_days",
        datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
    )
)

orders_enriched.write.mode("overwrite").parquet(f"{output_base}/orders_enriched")

monthly_sales = (
    orders_enriched
    .filter(col("order_purchase_timestamp").isNotNull())
    .withColumn("order_month", date_format(col("order_purchase_timestamp"), "yyyy-MM"))
    .groupBy("order_month")
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("payment_value"), 2).alias("total_revenue"),
        round(avg("payment_value"), 2).alias("average_payment")
    )
    .orderBy("order_month")
)

payment_summary = (
    orders_enriched
    .groupBy("payment_type")
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("payment_value"), 2).alias("total_revenue"),
        round(avg("payment_value"), 2).alias("average_payment")
    )
    .orderBy(col("total_revenue").desc())
)

category_sales = (
    orders_enriched
    .filter(col("product_category_name").isNotNull())
    .groupBy("product_category_name")
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("payment_value"), 2).alias("total_revenue"),
        round(avg("payment_value"), 2).alias("average_payment")
    )
    .orderBy(col("total_revenue").desc())
)

state_orders = (
    orders_enriched
    .groupBy("customer_state")
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("payment_value"), 2).alias("total_revenue")
    )
    .orderBy(col("total_orders").desc())
)

delivery_performance = (
    orders_enriched
    .filter(col("delivery_days").isNotNull())
    .groupBy("customer_state")
    .agg(
        count("order_id").alias("delivered_orders"),
        round(avg("delivery_days"), 2).alias("average_delivery_days")
    )
    .orderBy(col("average_delivery_days").desc())
)

monthly_sales.write.mode("overwrite").parquet(f"{output_base}/monthly_sales")
payment_summary.write.mode("overwrite").parquet(f"{output_base}/payment_summary")
category_sales.write.mode("overwrite").parquet(f"{output_base}/category_sales")
state_orders.write.mode("overwrite").parquet(f"{output_base}/state_orders")
delivery_performance.write.mode("overwrite").parquet(f"{output_base}/delivery_performance")

print("Phase 2 analytical datasets written successfully.")

spark.stop()