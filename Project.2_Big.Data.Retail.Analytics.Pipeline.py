
# ## **Project 2 – Big Data Retail Analytics Pipeline**

# ### **🎯 Objective**

# Build an **end-to-end data pipeline** that:

# * Ingests **massive transactional retail data**.
# * Cleans, transforms, and enriches it with customer data.
# * Performs **advanced aggregations, window functions, and joins**.
# * Optimizes performance with **partitions & caching**.
# * Stores the output in **Parquet & Delta**.
# * Produces **final business intelligence reports**.

# ---

# ### **🛠 Tools & Libraries**

# * **PySpark** (DataFrames, SQL, Window, UDFs)
# * **Faker** (to generate synthetic dataset)
# * **Parquet** / **Delta Lake**
# * **Jupyter Notebook** or `.py` script
# * Python standard libraries (`os`, `datetime`, etc.)

# ---

# ### **📂 Dataset**

# We’ll generate **3 datasets** using Faker:

# 1. **Transactions** – millions of sales records (transaction_id, customer_id, product_id, quantity, price, timestamp).
# 2. **Customers** – customer demographics (id, name, region, gender, signup_date).
# 3. **Products** – product catalog (id, name, category, subcategory, price).

# ---

# ### **🧩 Step-by-Step Methodology**

# #### **Step 1 – Data Generation**

# * Use Faker to generate:

#   * 5M+ transaction rows.
#   * 50k customer rows.
#   * 5k product rows.
# * Save them as CSV (raw data folder).

# #### **Step 2 – Data Ingestion**

# * Read CSV files into PySpark DataFrames.
# * Apply schema inference and column data type casting.

# #### **Step 3 – Data Cleaning**

# * Handle nulls.
# * Remove duplicate transaction IDs.
# * Filter out invalid prices and quantities.

# #### **Step 4 – Data Enrichment**

# * Join transactions with customers and products.
# * Add derived columns like:

#   * `total_sale = quantity * price`
#   * `year_month` from timestamp.

# #### **Step 5 – Aggregations**

# * **Top Customers by Region** (Window function RANK).
# * **Monthly Sales Trend** (group by year-month).
# * **Category-wise Revenue Contribution**.

# #### **Step 6 – Optimization**

# * **Cache** intermediate results for repeated queries.
# * **Repartition** by `region` and `year_month`.
# * Measure execution time before & after optimization.

# #### **Step 7 – Storage**

# * Save outputs in **Parquet** and **Delta** format in an `output/` folder.

# #### **Step 8 – Reporting**

# * Final reports:

#   1. Region-wise total revenue & top 5 customers.
#   2. Monthly revenue trend.
#   3. Category revenue share.

# ---

# ### **📜 Final Deliverables**

# * One `.py` or `.ipynb` file containing:

#   * All data generation code.
#   * ETL pipeline.
#   * Optimization steps.
#   * Storage & reporting.
# * Output files in **Parquet & Delta** format.
# * SQL queries run inside Spark.

# compact Capstone Script :
# Safe Defaults : DRY_RUN=True (no writes), USE_DELTA=False(no Delta config)
# Requires: pyspark, faker  (pip install pyspark faker)
# Optional: delta-spark (if USE_DELTA=True; pin a version compatible with Spark 3.3.x)

from pyspark.sql import SparkSession, functions as F 
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
from datetime import datetime, timedelta
from faker import Faker
import random, time
import os
import sys
# =========================================================
# FORCE SPARK_HOME to point ot the the correct installation path
# This is the definitive fix for the 'JavaPackage' error
# =========================================================
# The path should be up to the 'pyspark' folder within site-packages
os.environ['SPARK_HOME'] = r'D:\Uncodemy\Pyspark\pyspark_env\Lib\site-packages\pyspark'

# Set the Python interpreter for PySpark
os.environ['PYSPARK_PYTHON'] = r'D:\Uncodemy\Pyspark\pyspark_env\Scripts\python.exe'

# Ensure the Spark Libraries are on the path
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python/lib/py4j-0.10.9.5-src.zip'))
# =========================================================

# ---------------------- CONFIG ----------------------
# Scale knobs (increase to stress-test locally)
CUSTOMER_COUNT = 20_000
PRODUCT_COUNT  = 5_000
TXN_COUNT      = 300_000      # bump to 2_000_000+ when you’re ready

# Safety / output
DRY_RUN   = True              # True => do NOT write any files
USE_DELTA = False             # True => configure Delta & write Delta outputs
OUTPUT   = "out/p2_retail"    # used only if DRY_RUN=False

# Perf knobs
SHUFFLE_PARTITIONS = 16       # adjust by CPU cores / data size


# Purpose: Central “control panel” for the whole project.

# Key knobs:

# CUSTOMER_COUNT, PRODUCT_COUNT, TXN_COUNT → simulate dataset sizes for scaling.

# DRY_RUN=True → no writes happen (safe testing mode).

# USE_DELTA=False → keeps Delta Lake out unless explicitly needed.

# SHUFFLE_PARTITIONS → controls number of partitions Spark will create during shuffles (important for tuning performance).

# Why: Keeps your script flexible and reproducible (via SEED).

# Determinism
SEED = 42
random.seed(SEED)
Faker.seed(SEED)
fake = Faker()

# ---------------------- SPARK SESSION ----------------------
builder = (
    SparkSession.builder
    .appName("Project2-BigDataRetailAnalytics")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
)

if USE_DELTA:
    # Minimal Delta config (requires: pip install delta-spark==2.4.0 for Spark 3.3.x)
    builder = (
        builder
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

spark = builder.getOrCreate()
print(f"Spark {spark.version} | DRY_RUN={DRY_RUN} | USE_DELTA={USE_DELTA}")


# Purpose: Creates a Spark driver program, sets partition count, optionally enables Delta.

# .master("local[*]") → runs locally, using all CPU cores.

# .config("spark.sql.shuffle.partitions", ...) → prevents Spark’s default of 200 partitions (too many for small tests).

# Why: You now control both logical (code) and physical (execution) behavior.

# ---------------------- DIMENSIONS ----------------------
# Customers: id, name, age, gender, region
genders = ["Male", "Female", "Other"]
def map_region(country: str) -> str:
    if country in ("United States","Canada","Mexico"): return "North America"
    if country in ("United Kingdom","France","Germany","Italy","Spain"): return "Europe"
    if country in ("India","China","Japan","Singapore"): return "Asia"
    return "Other"

customers_py = []
for cid in range(1, CUSTOMER_COUNT + 1):
    country = fake.country()
    customers_py.append((
        cid,
        fake.name(),
        random.randint(18, 75),
        random.choice(genders),
        country,
        map_region(country)
    ))
schema_customers = "customer_id INT, customer_name STRING, age INT, gender STRING, country STRING, region STRING"
dim_customers = spark.createDataFrame(customers_py, schema_customers)

# Generated with Faker: customer_id, customer_name, age, gender, country, region.

# Why: Simulates a real dimension table in a data warehouse.

# map_region() turns raw country into analytical grouping (North America, Europe, Asia, Other).

# Products: id, category, subcategory, name, base_price
categories = ["Electronics","Clothing","Home & Kitchen","Sports","Toys"]
subcats = {
    "Electronics": ["Phone","Laptop","Headphones","Camera","Monitor"],
    "Clothing": ["Shirt","Jeans","Jacket","Shoes","Hoodie"],
    "Home & Kitchen": ["Blender","Cookware","Lamp","Desk","Chair"],
    "Sports": ["Ball","Gloves","Shoes","Watch","Mat"],
    "Toys": ["Doll","Blocks","Car","Puzzle","BoardGame"]
}
products_py = []
for pid in range(1, PRODUCT_COUNT + 1):
    cat = random.choice(categories)
    sub = random.choice(subcats[cat])
    products_py.append((
        pid, cat, sub, f"{cat}-{sub}-{pid:05d}", round(random.uniform(5, 1500), 2)
    ))
schema_products = "product_id INT, category STRING, subcategory STRING, product_name STRING, base_price DOUBLE"
dim_products = spark.createDataFrame(products_py, schema_products)

# Why: Allows category-level analysis, price-based metrics, etc.

# ---------------------- FACT: TRANSACTIONS (BRONZE) ----------------------
def rand_ts(days_back=270):
    # random datetime within ~9 months
    offset = random.randint(0, days_back)
    base = datetime.now() - timedelta(days=offset)
    return base.replace(hour=random.randint(0,23), minute=random.randint(0,59), second=random.randint(0,59), microsecond=0)

channels = ["Online","Store","Marketplace"]
txns_py = []
for oid in range(1, TXN_COUNT + 1):
    txns_py.append((
        f"TXN-{oid:010d}",
        rand_ts(),
        random.randint(1, CUSTOMER_COUNT),
        random.randint(1, PRODUCT_COUNT),
        random.randint(1, 5),
        random.choice(channels)
    ))
schema_txn = "txn_id STRING, txn_ts TIMESTAMP, customer_id INT, product_id INT, quantity INT, channel STRING"
txns_bronze = spark.createDataFrame(txns_py, schema_txn)

# Simulates raw event data:

# txn_id, txn_ts, customer_id, product_id, quantity, channel.

# Generated over ~9 months, with multiple channels (Online, Store, Marketplace).

# Why: The Bronze layer is raw & possibly messy — our pipeline will clean it.

# ---------------------- SILVER: ENRICH, CLEAN, PARTITION, CACHE ----------------------
t0 = time.perf_counter()

# Broadcast small dims -> map-side join (avoids shuffles on big fact)
silver = (
    txns_bronze
    .join(broadcast(dim_customers.select("customer_id","region","gender")), "customer_id", "left")
    .join(broadcast(dim_products.select("product_id","category","subcategory","base_price")), "product_id", "left")
    .withColumn("unit_price", F.col("base_price"))
    .withColumn("total_amount", F.col("unit_price") * F.col("quantity"))
    .withColumn("order_date", F.to_date("txn_ts"))
    .withColumn("yyyymm", F.date_format("txn_ts", "yyyyMM"))
)


# Joins:

# broadcast() → avoids expensive shuffles for small tables.

# Left joins → keep transactions even if dimension is missing.

# New columns:

# total_amount = price × qty.

# order_date → date without time (for daily grouping).

# yyyymm → monthly partition key.

# Cleaning:

# Filter out null prices or invalid quantities.

# .dropDuplicates(["txn_id"]) for idempotency.

# Performance:

# .repartition() → physical layout by month & region.

# .cache() → keep data in memory for next steps.

# Basic data hygiene
silver = (
    silver
    .filter(F.col("unit_price").isNotNull())  # drop unknown products
    .filter((F.col("quantity") > 0) & (F.col("unit_price") > 0))
    .dropDuplicates(["txn_id"])               # idempotency
)

# Physical layout for downstream shuffles & pruning
silver = silver.repartition(SHUFFLE_PARTITIONS, F.col("yyyymm"), F.col("region")).cache()
_ = silver.count()  # materialize cache

t1 = time.perf_counter()
print(f"Silver ready in {t1 - t0:.2f}s | rows={silver.count()} | partitions={silver.rdd.getNumPartitions()}")

# ---------------------- GOLD: BUSINESS METRICS ----------------------
# 1) Monthly revenue by region & category
gold_monthly_region_cat = (
    silver.groupBy("yyyymm","region","category")
          .agg(F.sum("total_amount").alias("revenue"),
               F.sum("quantity").alias("units"))
)

# 2) Top 10 customers per region by spend (dense_rank window)
cust_spend = silver.groupBy("region","customer_id").agg(F.sum("total_amount").alias("total_spent"))
win = Window.partitionBy("region").orderBy(F.desc("total_spent"))
gold_top_customers = cust_spend.withColumn("rank", F.dense_rank().over(win)).filter(F.col("rank") <= 10)

# 3) Monthly revenue + 3-month moving average
monthly_total = silver.groupBy("yyyymm").agg(F.sum("total_amount").alias("revenue"))
win_ma = Window.orderBy("yyyymm").rowsBetween(-2, 0)
gold_rev_trend = monthly_total.withColumn("rev_ma_3m", F.avg("revenue").over(win_ma))

# ---------------------- OPTIONAL OUTPUT (PARQUET/DELTA) ----------------------
if not DRY_RUN:
    if USE_DELTA:
        fmt = "delta"
        mode = "overwrite"
        (silver.write.format(fmt).mode(mode).partitionBy("yyyymm","region").save(f"{OUTPUT}/silver_orders"))
        gold_monthly_region_cat.write.format(fmt).mode(mode).save(f"{OUTPUT}/gold_monthly_region_cat")
        gold_top_customers.write.format(fmt).mode(mode).save(f"{OUTPUT}/gold_top_customers")
        gold_rev_trend.write.format(fmt).mode(mode).save(f"{OUTPUT}/gold_rev_trend")
    else:
        (silver.write.mode("overwrite").partitionBy("yyyymm","region").parquet(f"{OUTPUT}/silver_orders"))
        gold_monthly_region_cat.write.mode("overwrite").parquet(f"{OUTPUT}/gold_monthly_region_cat")
        gold_top_customers.write.mode("overwrite").parquet(f"{OUTPUT}/gold_top_customers")
        gold_rev_trend.write.mode("overwrite").parquet(f"{OUTPUT}/gold_rev_trend")

# ---------------------- SAMPLE DISPLAYS (NO WRITE) ----------------------
print("\n=== SILVER sample ===")
silver.select("txn_id","order_date","region","category","quantity","unit_price","total_amount","yyyymm").show(5, truncate=False)

print("\n=== GOLD: monthly revenue by region/category (sample) ===")
gold_monthly_region_cat.orderBy("yyyymm","region","category").show(10, truncate=False)

print("\n=== GOLD: Top Customers per region (sample) ===")
gold_top_customers.orderBy("region","rank").show(10, truncate=False)

print("\n=== GOLD: Revenue trend with 3M MA (sample) ===")
gold_rev_trend.orderBy("yyyymm").show(10, truncate=False)

spark.stop()


# Keep DRY_RUN = True while testing.

# When you’re happy and have disk space, set DRY_RUN = False to save Parquet (or Delta if you set USE_DELTA = True).

# To stress test, increase TXN_COUNT (e.g., 2,000,000) and bump SHUFFLE_PARTITIONS.


# ✅ This script applies EVERYTHING from the PySpark course:

# SparkSession configs

# Broadcast joins

# Partition tuning

# Cache & count materialization

# Window functions (dense_rank, moving average)

# Partitioned writes (Parquet/Delta)

# Multi-step pipeline (Bronze → Silver → Gold)



# Spark UI checklist:

# 1️⃣ Jobs Tab

# What to check:

# Number of jobs: Expect 4–6 total (Bronze load, Silver join, Gold aggregations, sample .show() calls).

# Job names: Look for ParquetWrite, ShuffledHashJoin, and SortAggregate.

# Why: Shows how logical DataFrame ops are broken into physical jobs.


# 2️⃣ Stages Tab

# What to check:

# Shuffle Read/Write (MB):

# Small for broadcast joins (a few KB or MB).

# Larger for Gold-level aggregations (groupBy).

# Number of Tasks:

# Should match or be a multiple of SHUFFLE_PARTITIONS=16.

# Why: Confirms partition tuning worked.


# 3️⃣ Storage Tab

# What to check:

# After .cache() in the Silver layer, you should see Silver DataFrame persisted.

# Memory usage should be visible (in MB).

# Why: Confirms caching is actually working.


# 4️⃣ SQL Tab

# What to check:

# Query plans for each DataFrame action.

# Look for:

# BroadcastHashJoin → means broadcast hint was applied.

# WholeStageCodegen → means Spark optimized code paths.

# Why: Confirms Spark’s Catalyst optimizer did its job.



# 5️⃣ Executors Tab

# What to check:

# Number of cores in use: Should match your local[*] setup.

# Task time distribution — no single task should take much longer than the others (good partition balance).

# Why: Checks for skewed data or uneven workload.



# 6️⃣ Environment Tab

# What to check:

# spark.sql.shuffle.partitions=16

# Delta configs if USE_DELTA=True.

# Why: Confirms your script’s config settings took effect.


# 📌 Prime Tip
# Befoe running the project set : DRY_RUN = True
# This ensures no files are written during testing.
# Once you’re satisfied with the output samples and performance,
# set DRY_RUN = False to write Parquet (or Delta if USE_DELTA = True) files to disk.




