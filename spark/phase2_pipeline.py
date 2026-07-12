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

print("Phase 2 base enriched table written successfully.")

spark.stop()