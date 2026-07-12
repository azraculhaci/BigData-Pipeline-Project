# Big Data Analytics Pipeline — Olist E-Commerce

A hands-on big data project built around the
[Olist Brazilian E-Commerce public dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) —
~100,000 real orders from Brazil's largest online marketplace (2016–2018).

The project is developed in stages. Each stage adds a new layer to the pipeline. More stages will be added over time.

---

## 📌 Important Notes

### Submission
Each student must **fork or clone this repository**, implement their solution, and submit by **opening a Pull Request (PR) back to this repository** with their completed work. PRs are the only accepted submission method.

### Docker is Optional
The Docker Compose files and scripts provided in this repo are **starter code only** — a reference setup to help you get up and running quickly. You are **not required** to use Docker. Feel free to run HDFS, Spark, and Superset however you prefer (local install, cloud, a different container setup, etc.), as long as the pipeline works end-to-end.

---

## Architecture (Phase 1)

```
[Olist Dataset — 9 CSV Tables]
        |
        v
[Apache Spark]
  · Reads CSVs
  · Writes Parquet
        |
        v
[HDFS or MinIO]
        |
        v
[Apache Superset — Simple Charts]
```

---


## Phases

### ✅ Phase 1 — Ingest & Visualize

> **Current task**

- Download the Olist dataset (9 CSV tables).
- Import all CSVs into **HDFS or MinIO** in **Parquet format** using Apache Spark.
- Connect Apache Superset to the stored data and create a few simple charts/diagrams.

No advanced transformations are required for this phase.

---

### 🔜 Phase 2 — Coming Soon

Details will be announced.

---

## Docker Quick Start (Optional)

The following commands use the provided Docker Compose files as a starting point.

**1. Create the shared network**

```bash
# Linux / macOS
bash scripts/setup_network.sh

# Windows (PowerShell)
.\scripts\setup_network.ps1
```

**2. Start the services**

```bash
docker compose -f docker/docker-compose-hdfs.yml up -d
docker compose -f docker/docker-compose-spark.yml up -d
docker compose -f docker/docker-compose-superset.yml up -d
```

| Service         | URL                       | Credentials   |
|-----------------|---------------------------|---------------|
| HDFS NameNode   | http://localhost:9870     |               |
| Spark Master    | http://localhost:8080     |               |
| Superset        | http://localhost:8088     | admin / admin |

**3. Stop everything**

```bash
docker compose -f docker/docker-compose-superset.yml down
docker compose -f docker/docker-compose-spark.yml down
docker compose -f docker/docker-compose-hdfs.yml down
```
## My Phase 1 Implementation

This implementation completes the first stage of the Olist big data analytics pipeline. The raw CSV files were loaded into HDFS, converted to Parquet with Apache Spark, and visualized with Apache Superset.

### Completed Work

- Downloaded the 9 original Olist CSV tables.
- Uploaded the raw CSV files to HDFS.
- Converted all CSV tables to Parquet format using Apache Spark.
- Stored the Parquet outputs in HDFS.
- Created small dashboard export datasets for Superset.
- Built a Superset dashboard with simple Phase 1 visualizations.

### HDFS Paths

Raw CSV files:

```text
/data/olist/raw
```

Parquet outputs:

```text
/data/olist/parquet
```

Superset export datasets:

```text
/data/olist/superset_exports
```

### Spark Jobs

#### CSV to Parquet Ingestion

```text
spark/ingest_to_parquet.py
```

This script reads the 9 raw Olist CSV files from HDFS and writes each table back to HDFS in Parquet format.

#### Dashboard Export Builder

```text
spark/build_dashboard_exports.py
```

This script reads selected Parquet tables and creates lightweight CSV exports for Superset visualizations.

Generated export datasets:

```text
order_status_breakdown
daily_order_volume
payment_method_mix
top_20_product_categories
```

### Local Export Files

The Superset-ready CSV files are stored locally under:

```text
reports/superset_exports/
```

Included files:

```text
order_status_breakdown.csv
daily_order_volume.csv
payment_method_mix.csv
top_20_product_categories.csv
```

### Superset Dashboard

Dashboard name:

```text
Olist E-Commerce Phase 1 Overview
```

Charts included:

- Order Status Breakdown
- Daily Order Volume
- Payment Method Mix
- Top 20 Product Categories

### Pipeline Summary

```text
Olist CSV files
      |
      v
HDFS raw storage
      |
      v
Spark CSV to Parquet job
      |
      v
HDFS Parquet storage
      |
      v
Spark dashboard export job
      |
      v
Superset CSV datasets and dashboard
```
## Phase 2 - Simple Data Pipeline

In this phase, a simple Spark data pipeline was implemented for the Olist E-Commerce dataset.

The pipeline reads the raw CSV files, processes them with Apache Spark, and writes the output as Parquet files. This creates a reusable data layer for later analytics and visualization steps.

### Pipeline Steps

1. Read the Olist CSV files from `data/raw/`.
2. Load the files into Apache Spark DataFrames.
3. Apply basic schema inference and ingestion metadata.
4. Write each table as Parquet output.
5. Store the generated files in the project output/data layer.

### Input Dataset

The following Olist CSV tables are used:

- `olist_customers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `product_category_name_translation.csv`

### Output

The processed datasets are written in Parquet format.

Example output structure:

```text
data/processed/
├── customers/
├── geolocation/
├── order_items/
├── order_payments/
├── order_reviews/
├── orders/
├── products/
├── sellers/
└── product_category_translation/
```

# How to Run

Start the required services:

```bash
docker compose -f docker/docker-compose-spark.yml up -d
docker compose -f docker/docker-compose-hdfs.yml up -d
docker compose -f docker/docker-compose-dev.yml up -d --build
```

Run the ingestion pipeline:

```bash
docker exec -it olist-dev python processing/ingest_to_bronze.py
```

# Verification

After running the pipeline, verify that the Parquet files were successfully written to HDFS:

```bash
docker exec namenode hdfs dfs -ls /olist/bronze
```

Expected output:

```text
/olist/bronze/customers
/olist/bronze/geolocation
/olist/bronze/order_items
/olist/bronze/order_payments
/olist/bronze/order_reviews
/olist/bronze/orders
/olist/bronze/products
/olist/bronze/sellers
/olist/bronze/product_category_translation
```