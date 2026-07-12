from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Olist CSV to Parquet")
    .getOrCreate()
)

tables = [
    "olist_customers_dataset",
    "olist_geolocation_dataset",
    "olist_order_items_dataset",
    "olist_order_payments_dataset",
    "olist_order_reviews_dataset",
    "olist_orders_dataset",
    "olist_products_dataset",
    "olist_sellers_dataset",
    "product_category_name_translation",
]

input_base = "hdfs://namenode:9000/data/olist/raw"
output_base = "hdfs://namenode:9000/data/olist/parquet"

for table in tables:
    input_path = f"{input_base}/{table}.csv"
    output_path = f"{output_base}/{table}"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    df.write.mode("overwrite").parquet(output_path)
    print(f"Written {table} to {output_path}")

spark.stop()