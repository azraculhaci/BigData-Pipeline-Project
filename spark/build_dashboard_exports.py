from pyspark.sql import SparkSession
from pyspark.sql.functions import count, to_date

spark = (
    SparkSession.builder
    .appName("Olist Phase 1 - Dashboard Export Builder")
    .getOrCreate()
)

parquet_base = "hdfs://namenode:9000/data/olist/parquet"
export_base = "hdfs://namenode:9000/data/olist/superset_exports"

orders = spark.read.parquet(f"{parquet_base}/olist_orders_dataset")
payments = spark.read.parquet(f"{parquet_base}/olist_order_payments_dataset")
products = spark.read.parquet(f"{parquet_base}/olist_products_dataset")

order_status_breakdown = (
    orders
    .groupBy("order_status")
    .agg(count("*").alias("total_orders"))
    .orderBy("order_status")
)

daily_order_volume = (
    orders
    .withColumn("order_date", to_date("order_purchase_timestamp"))
    .groupBy("order_date")
    .agg(count("*").alias("total_orders"))
    .orderBy("order_date")
)

payment_method_mix = (
    payments
    .groupBy("payment_type")
    .agg(count("*").alias("payment_records"))
    .orderBy("payment_type")
)

top_product_categories = (
    products
    .groupBy("product_category_name")
    .agg(count("*").alias("product_count"))
    .orderBy("product_count", ascending=False)
    .limit(20)
)

exports = {
    "order_status_breakdown": order_status_breakdown,
    "daily_order_volume": daily_order_volume,
    "payment_method_mix": payment_method_mix,
    "top_20_product_categories": top_product_categories,
}

for export_name, export_df in exports.items():
    output_path = f"{export_base}/{export_name}"
    (
        export_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )
    print(f"Exported {export_name} to {output_path}")

spark.stop()