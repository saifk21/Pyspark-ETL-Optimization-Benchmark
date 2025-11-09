# We already prepared a script that compares:

# Unoptimized pipeline → normal joins, no caching, default partitions.

# Optimized pipeline → broadcast joins, repartitioning, caching.

from pyspark.sql import SparkSession
import time
from faker import Faker
import pandas as pd
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



spark = SparkSession.builder \
    .appName("PySparkOptimizationBenchmark") \
    .getOrCreate()

fake = Faker()

# Generate small fake dataset for testing
data = [(i, fake.name(), fake.state(), fake.random_int(100, 1000))
        for i in range(1, 500001)]
df = spark.createDataFrame(data, ["id", "name", "state", "sales"])

# Simulate another table for joins
data2 = [(fake.state(), fake.random_int(1, 5))
         for _ in range(50)]
df2 = spark.createDataFrame(data2, ["state", "category"])

# --- Unoptimized run ---
start = time.time()
df.join(df2, on="state").groupBy("category").sum("sales").collect()
end = time.time()
unoptimized_time = end - start

# --- Optimized run ---
from pyspark.sql.functions import broadcast

start = time.time()
df.join(broadcast(df2), on="state") \
  .repartition("category") \
  .groupBy("category").sum("sales") \
  .cache() \
  .collect()
end = time.time()
optimized_time = end - start

print(f"⏳ Unoptimized: {unoptimized_time:.2f} sec")
print(f"⚡ Optimized: {optimized_time:.2f} sec")



#  Interpreting the Results

# When you run the above:

# Check the timings: Optimized should be noticeably faster.

# Understand why:

# Broadcast join avoids shuffling large tables.

# Repartition aligns the data distribution with your groupBy key.

# Cache speeds up repeated access.

# Example output:

# ⏳ Unoptimized: 12.54 sec
# ⚡ Optimized: 4.87 sec
# That’s ~2.5× faster with the same output.




  

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




# PySpark Script to Generate Large Synthetic Datasets (Parquet)
# This script must be run BEFORE the benchmarking script.

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from faker import Faker
import random
from uuid import uuid4
import os
import sys
import builtins # <-- ADDED: Needed to explicitly call the Python built-in round()

# =========================================================
# ESSENTIAL ENVIRONMENT CONFIGURATION (from previous steps)
# =========================================================
os.environ['SPARK_HOME'] = r'D:\Uncodemy\Pyspark\pyspark_env\Lib\site-packages\pyspark'
os.environ['PYSPARK_PYTHON'] = r'D:\Uncodemy\Pyspark\pyspark_env\Scripts\python.exe'
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python'))
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'python/lib/py4j-0.10.9.5-src.zip'))

# ----------------------------------------
# Project Configuration (Aligned with your needs)
# ----------------------------------------
CUSTOMER_COUNT = 10_000      # 10k unique customers
PRODUCT_COUNT = 3_000        # 3k unique products
ORDER_COUNT = 200_000        # 200k transactions (will be the large DataFrame)
RANDOM_SEED = 42
DRY_RUN = False              # MUST be False to write files!

# Output base folder
INPUT_BASE = r"D:\Uncodemy\Pyspark\Project.2_ETL.Benchmarking"

# Ensure the base directory exists
os.makedirs(INPUT_BASE, exist_ok=True)

# ===== Create Spark Session =====
spark = SparkSession.builder \
    .appName("DataGenerator") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
    
# Set up Faker for synthetic data generation
fake = Faker()
random.seed(RANDOM_SEED)

# ----------------------------------------
# 1. CUSTOMERS Data Generation
# ----------------------------------------

def generate_customers():
    print(f"Generating {CUSTOMER_COUNT:,} Customers...")
    regions = ["North", "South", "East", "West", "Central"]
    
    customer_data = []
    for _ in range(CUSTOMER_COUNT):
        customer_data.append({
            "customer_id": str(uuid4()),
            "name": fake.name(),
            "city": fake.city(),
            "region": random.choice(regions)
        })

    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("region", StringType(), True),
    ])
    
    return spark.createDataFrame(customer_data, schema).repartition(2) # Keep dimension tables small

# ----------------------------------------
# 2. PRODUCTS Data Generation
# ----------------------------------------

def generate_products():
    print(f"Generating {PRODUCT_COUNT:,} Products...")
    categories = ["Electronics", "Clothing", "Home", "Books", "Toys", "Groceries"]
    
    product_data = []
    for _ in range(PRODUCT_COUNT):
        product_data.append({
            "product_id": str(uuid4()),
            "name": fake.word().capitalize(),
            "category": random.choice(categories),
            # FIX: Use builtins.round() to avoid conflict with pyspark.sql.functions.round
            "price": builtins.round(random.uniform(10.00, 150.00), 2)
        })

    schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), False),
    ])
    
    return spark.createDataFrame(product_data, schema).repartition(1)

# ----------------------------------------
# 3. TRANSACTIONS Data Generation
# ----------------------------------------

def generate_transactions(customer_ids, product_ids):
    print(f"Generating {ORDER_COUNT:,} Transactions...")
    
    # Generate pure Python list for parallelization
    transaction_data = []
    from datetime import datetime, timedelta # Need to import these here since they weren't globally
    start_date = datetime.strptime("2024-01-01", "%Y-%m-%d")
    end_date = datetime.strptime("2025-08-31", "%Y-%m-%d")
    
    for i in range(ORDER_COUNT):
        # Use existing IDs for foreign keys
        cust_id = random.choice(customer_ids)
        prod_id = random.choice(product_ids)
        
        # Random date between start and end
        time_diff = end_date - start_date
        random_days = random.randrange(time_diff.days)
        txn_date = start_date + timedelta(days=random_days)
        
        transaction_data.append((
            str(uuid4()), # transaction_id
            cust_id,
            prod_id,
            random.randint(1, 5), # quantity
            txn_date.strftime("%Y-%m-%d") # transaction_date
        ))

    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("transaction_date", StringType(), False),
    ])
    
    # Create DF and partition it into 8 slices for better parallelism in the next step
    return spark.createDataFrame(transaction_data, schema).repartition(8) 


# ----------------------------------------
# MAIN EXECUTION
# ----------------------------------------

# 1. Generate Dimension DataFrames
df_customers = generate_customers()
df_products = generate_products()

# 2. Extract IDs for Foreign Keys
customer_ids = [row.customer_id for row in df_customers.collect()]
product_ids = [row.product_id for row in df_products.collect()]

# 3. Generate Fact DataFrames
df_transactions = generate_transactions(customer_ids, product_ids)

# 4. Write DataFrames as Parquet (only if DRY_RUN is False)
if not DRY_RUN:
    print("\n--- Writing Parquet Files ---")
    
    # Customers
    customers_path = f"{INPUT_BASE}/customers"
    # Ensure all three output directories are created by the write operation.
    df_customers.write.mode("overwrite").parquet(customers_path)
    print(f"Customers written to: {customers_path}")
    
    # Products
    products_path = f"{INPUT_BASE}/products"
    df_products.write.mode("overwrite").parquet(products_path)
    print(f"Products written to: {products_path}")

    # Transactions (The main large table)
    transactions_path = f"{INPUT_BASE}/transactions"
    # Note: Using .repartition(16) before writing is a good practice for big files
    df_transactions.repartition(16).write.mode("overwrite").parquet(transactions_path)
    print(f"Transactions written to: {transactions_path}")

else:
    print("\nDRY_RUN is TRUE. No files were written to disk.")

spark.stop()
print("\nData generation complete.")
# We’ll benchmark the ETL in three modes:

# No optimization (baseline) — no broadcast joins, no cache, default shuffle partitions.

# Partial optimization — broadcast joins only.

# Full optimization — broadcast joins + cache + tuned repartition.

# We’ll capture execution time, shuffle read/write sizes, and Spark UI join types for each.



# 📊 Step-by-step Performance Test Plan
# 1️⃣ Prepare the environment

# Ensure Bronze dataset is big enough (≥ 1M transactions).
# If not, re-run the generator with more rows:

# --num_txns 2000000 --num_customers 50000 --num_products 2000 --dry_run False
# Keep INPUT_BASE consistent across tests.

# 2️⃣ Create three ETL scripts

# We’ll clone etl_project2.py into three variants:

# a) Baseline (no optimization)

# Remove broadcast(...) calls → just .join(dim_customers_small, ...).

# Remove .persist() call on silver.

# Remove .repartition(...) call.

# Keep default Spark shuffle partitions (spark.sql.shuffle.partitions=200).


# b) Broadcast only

# Keep broadcast(dim_customers_small) and broadcast(dim_products_small).

# No .persist() or .repartition().


# c) Full optimization

# Keep broadcast(...).

# Keep .persist(StorageLevel.MEMORY_AND_DISK).

# Keep .repartition(SHUFFLE_PARTITIONS, col("yyyymm"), col("region")) with tuned partitions (e.g., 16 or 32).


# Open Spark UI for each run

# Go to http://localhost:4040
#  while it’s running.

# Note:

# Join type in SQL plan (BroadcastHashJoin vs SortMergeJoin).

# Shuffle Read / Shuffle Write MB.

# Number of tasks and stages.

# GC (garbage collection) time if dataset is big.


# Compare results

# Make a table:

# Mode	Wall time (s)	Join type	Shuffle Read MB	Shuffle Write MB
# Baseline				
# Broadcast only				
# Full optimization				

# Expectations:

# Broadcast only → big drop in shuffle read/write.

# Full optimization → further drop in wall time due to less recomputation and better partitioning.


# 6️⃣ Optional: Run on bigger dataset

# Scale up to 10M transactions to see bigger differences.

# Ensure you have enough RAM or tune shuffle partitions accordingly.

# Now lets make timing harness to automate the three runs and capture metrics programmatically.

# Performance benchmarking harness for Project 2 to test
#  1.) Baseline ETL (no optimizations)
#  2.) ETL with broadcast joins only
#  3.) ETL with full optimizations (broadcast joins + caching + tuned repartitioning)

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from pyspark import StorageLevel
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

# ----------------------------------------
# Configurations
# ----------------------------------------

INPUT_BASE = r"D:\Uncodemy\Pyspark\Project.2_ETL.Benchmarking"
SHUFFLE_PARTITIONS = 32  # Tuned number of shuffle partitions for full optimization



# Modes
MODES = ["baseline", "broadcast", "full"]

# ----------------------------------------
# Common Spark session
# ----------------------------------------
spark = SparkSession.builder \
    .appName("PySpark Optimization Benchmark") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# ----------------------------------------
# Load data
# ----------------------------------------
transactions = spark.read.parquet(f"{INPUT_BASE}/transactions")
customers = spark.read.parquet(f"{INPUT_BASE}/customers")
products = spark.read.parquet(f"{INPUT_BASE}/products")

# Reduce customers/products size for broadcast example
dim_customers_small = customers.limit(5000)
dim_products_small = products.limit(2000)

# ----------------------------------------
# Benchmark function
# ----------------------------------------
def run_etl(mode):
    print(f"\n=== Running mode: {mode.upper()} ===")

    start_time = time.time()

    # Join logic based on mode
    if mode == "baseline":
        silver = transactions \
            .join(customers, "customer_id") \
            .join(products, "product_id")

    elif mode == "broadcast":
        silver = transactions \
            .join(broadcast(dim_customers_small), "customer_id") \
            .join(broadcast(dim_products_small), "product_id")

    elif mode == "full":
        silver = transactions \
            .join(broadcast(dim_customers_small), "customer_id") \
            .join(broadcast(dim_products_small), "product_id") \
            .persist(StorageLevel.MEMORY_AND_DISK) \
            .repartition(SHUFFLE_PARTITIONS, col("region"))

    # Example aggregation
    agg_df = silver.groupBy("region").count()
    agg_df.collect()  # Force execution

    elapsed = time.time() - start_time
    print(f"Elapsed time: {elapsed:.2f} seconds")
    return elapsed

# ----------------------------------------
# Run benchmarks
# ----------------------------------------
results = {}
for mode in MODES:
    results[mode] = run_etl(mode)

# ----------------------------------------
# Print comparison table
# ----------------------------------------
print("\n=== Benchmark Results ===")
print(f"{'Mode':<15} {'Time (s)':<10}")
print("-" * 25)
for mode, t in results.items():
    print(f"{mode:<15} {t:<10.2f}")

spark.stop()


# extend this harness to pull shuffle read/write metrics directly from Spark’s listener API so you get both 
# time and shuffle sizes in one table. That way you won’t need to manually open Spark UI.


# 📌 Where we are in Project 2

# Data Generation ✅

# Bronze → Silver → Gold transformations ✅

# Optimizations ✅

# Benchmarking & Comparison ⬅ (current step, final)

# Final project summary & wrap-up 🏁

# Project 2 – Big Data Sales Analytics Pipeline

# Objective:
# Simulate and analyze large-scale sales transactions to uncover trends, customer behavior, and product 
# performance — with PySpark optimizations applied.

# Step-by-Step Plan
# 1️⃣ Data Generation

# Use Faker to simulate:

# Customers (ID, Name, Region, Age, Gender)

# Products (ID, Category, Price)

# Transactions (CustomerID, ProductID, Quantity, Timestamp)

# Generate a large dataset (1–5 million rows).

# 2️⃣ Data Loading

# Read generated CSV/Parquet into Spark DataFrames.

# Apply schema inference and ensure correct data types.

# 3️⃣ Data Cleaning

# Handle nulls in Customer or Product data.

# Remove transactions with negative or zero quantities.

# Standardize text fields (capitalize names, etc.).

# 4️⃣ Data Transformation

# Join Transactions with Customer and Product tables.

# Add TotalSale = Price × Quantity column.

# Extract Month, Year from timestamps.

# 5️⃣ Analysis Stage

# Top Customers by total spend (per region).

# Top Products by total sales.

# Monthly Sales Trends.

# Sales by Age Group.

# 6️⃣ Optimization Stage

# Repartition data intelligently.

# Cache frequently used DataFrames.

# Use column pruning and predicate pushdown.

# Compare execution time before & after.

# 7️⃣ Reporting

# Save results to Parquet.

# Load into a Spark SQL view and run SQL queries.

# Output summaries for business reporting.

# 8️⃣ Deliverables

# A single PySpark script (compact but clear).

# Before/After performance metrics.

# Practice questions at the end.




# 🔥 Step 1 — Scalable Faker data generator (Bronze data)

# Generates dim_customers, dim_products, and txn_transactions using Spark parallelism
# (no huge in-memory Python lists).

# Default sizes are modest; you can scale up.

# DRY_RUN prevents writing; set DRY_RUN = False to persist Parquet files.



"""

Scalable data generator for Project 2 (PySpark).
By default: DRY_RUN = True -> no files are written.
To persist outputs: set DRY_RUN = False and ensure OUTPUT_BASE has free space.

"""

from pyspark.sql import SparkSession, functions as F, types as T
from faker import Faker
import random
from datetime import datetime, timedelta
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


# ------------------ CONFIG ------------------
CUSTOMER_COUNT = 50_000        # 50k customers (adjustable)
PRODUCT_COUNT  = 5_000         # 5k products
TXN_COUNT      = 1_000_000     # 1M transactions by default (scale up to 5M+ if disk/CPU allow)
PARTITIONS     = 8             # number of parallel partitions used for generation
DRY_RUN        = True          # True = do NOT write files; False = will write Parquet to OUTPUT_BASE
OUTPUT_BASE    = r"D:\Uncodemy\Pyspark\Project.2_Version.2.Big.Data.Retail.Analytics.Pipeline"   # only used when DRY_RUN=False
SEED           = 42

# ------------------ SPARK ------------------
spark = SparkSession.builder \
    .appName("Project2_DataGenerator") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", str(PARTITIONS)) \
    .getOrCreate()

sc = spark.sparkContext
random.seed(SEED)
Faker.seed(SEED)
fake = Faker()

# ------------------ UTILS: helper functions that run inside mapPartitions ------------------
def gen_customers(pid_range):
    """Generate customers in a partition (pid_range is an iterable of integer ids)."""
    f = Faker()
    random.seed(SEED + 1)  # deterministic-ish per worker
    Faker.seed(SEED + 1)
    genders = ["Male", "Female", "Other"]
    for cid in pid_range:
        country = f.country()
        # coarse region mapping (example)
        if country in ("United States", "Canada", "Mexico"):
            region = "North America"
        elif country in ("United Kingdom","France","Germany","Italy","Spain"):
            region = "Europe"
        elif country in ("India","China","Japan","Singapore"):
            region = "Asia"
        else:
            region = "Other"
        yield (int(cid),
               f.name(),
               int(random.randint(18, 80)),
               random.choice(genders),
               country,
               region)

def gen_products(pid_range):
    """Generate product rows."""
    f = Faker()
    random.seed(SEED + 2)
    categories = ["Electronics","Clothing","Home & Kitchen","Sports","Toys"]
    subcats = {
        "Electronics": ["Phone","Laptop","Headphones","Camera","Monitor"],
        "Clothing": ["Shirt","Jeans","Jacket","Shoes","Hoodie"],
        "Home & Kitchen": ["Blender","Cookware","Lamp","Desk","Chair"],
        "Sports": ["Ball","Gloves","Shoes","Watch","Mat"],
        "Toys": ["Doll","Blocks","Car","Puzzle","BoardGame"]
    }
    for pid in pid_range:
        cat = random.choice(categories)
        sub = random.choice(subcats[cat])
        name = f"{cat}-{sub}-{pid:05d}"
        price = round(random.uniform(5.0, 1500.0), 2)
        yield (int(pid), cat, sub, name, float(price))

def gen_txns(index_iter, customer_count, product_count, channels=("Online","Store","Marketplace")):
    """Generate transactions from an iterator of integers (index_iter)."""
    f = Faker()
    random.seed(SEED + 3)
    for i in index_iter:
        txn_id = f"TXN-{i:010d}"
        # random datetime within last 270 days
        offset = random.randint(0, 270)
        base = datetime.now() - timedelta(days=offset)
        txn_ts = base.replace(hour=random.randint(0,23), minute=random.randint(0,59),
                              second=random.randint(0,59), microsecond=0)
        cust = random.randint(1, customer_count)
        prod = random.randint(1, product_count)
        qty = random.randint(1, 5)
        ch = random.choice(channels)
        yield (txn_id, txn_ts, cust, prod, qty, ch)

# ------------------ SCHEMAS ------------------
schema_customers = T.StructType([
    T.StructField("customer_id", T.IntegerType(), False),
    T.StructField("customer_name", T.StringType(), True),
    T.StructField("age", T.IntegerType(), True),
    T.StructField("gender", T.StringType(), True),
    T.StructField("country", T.StringType(), True),
    T.StructField("region", T.StringType(), True),
])

schema_products = T.StructType([
    T.StructField("product_id", T.IntegerType(), False),
    T.StructField("category", T.StringType(), True),
    T.StructField("subcategory", T.StringType(), True),
    T.StructField("product_name", T.StringType(), True),
    T.StructField("base_price", T.DoubleType(), True),
])

schema_txns = T.StructType([
    T.StructField("txn_id", T.StringType(), False),
    T.StructField("txn_ts", T.TimestampType(), True),
    T.StructField("customer_id", T.IntegerType(), True),
    T.StructField("product_id", T.IntegerType(), True),
    T.StructField("quantity", T.IntegerType(), True),
    T.StructField("channel", T.StringType(), True),
])

# ------------------ GENERATE DIMENSIONS (customers, products) ------------------
# We create an RDD of ids and mapPartitions to generate rows without collecting huge lists in driver.

# customers
cust_rdd = sc.parallelize(range(1, CUSTOMER_COUNT + 1), PARTITIONS)
cust_rows_rdd = cust_rdd.mapPartitions(gen_customers)
dim_customers = spark.createDataFrame(cust_rows_rdd, schema_customers)
print("Customers schema and sample:")
dim_customers.printSchema()
dim_customers.show(3, truncate=False)

# products
prod_rdd = sc.parallelize(range(1, PRODUCT_COUNT + 1), PARTITIONS)
prod_rows_rdd = prod_rdd.mapPartitions(gen_products)
dim_products = spark.createDataFrame(prod_rows_rdd, schema_products)
print("Products schema and sample:")
dim_products.printSchema()
dim_products.show(3, truncate=False)

# ------------------ GENERATE TRANSACTIONS (txns) ------------------
# We create an RDD of integers up to TXN_COUNT and generate transactions in partitions.
idx_rdd = sc.parallelize(range(1, TXN_COUNT + 1), PARTITIONS)
txns_rdd = idx_rdd.mapPartitions(lambda it: gen_txns(it, CUSTOMER_COUNT, PRODUCT_COUNT))
txns_df = spark.createDataFrame(txns_rdd, schema_txns)
print("Transactions schema and a small sample:")
txns_df.printSchema()
txns_df.show(5, truncate=False)

# ------------------ DRY_RUN: avoid writes unless explicitly allowed ------------------
if not DRY_RUN:
    # write Bronze CSV/Parquet (partition lightly)
    txns_df.write.mode("overwrite").partitionBy(F.year("txn_ts").alias("year"), F.month("txn_ts").alias("month")).parquet(f"{OUTPUT_BASE}/txns")
    dim_customers.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/dim_customers")
    dim_products.write.mode("overwrite").parquet(f"{OUTPUT_BASE}/dim_products")
    print("Wrote Parquet to", OUTPUT_BASE)
else:
    print("DRY_RUN = True -> no files written. Set DRY_RUN = False to persist outputs.")

# ------------------ CLEANUP ------------------
spark.stop()


# 📘 Why this approach?

# Scales: generation happens in Spark partitions (mapPartitions), not collecting lists in Python driver — 
# so you can generate millions of rows without exhausting memory.

# Deterministic-ish: we seed random and Faker so results are reproducible across runs when same seed used.

# Safe-by-default: DRY_RUN=True prevents accidental writes to disk.

# Parquet-ready: when you set DRY_RUN=False, the code writes partitioned Parquet for efficient downstream
# processing.


# 🧱 👨🏫 What each main block does (plain words)

# Config: lets you change counts, partitions, and toggle writes.

# Spark session: boots Spark and sets shuffle partitions to match parallelism.

# Generators (gen_customers, gen_products, gen_txns): functions that produce rows inside executors 
# (mapPartitions).

# Create RDDs of ids: parallelize(range(...), PARTITIONS) creates partitions of integer ids.

# Map partitions: convert ids into rows using Faker inside each executor.

# Build DataFrames: wrap RDD rows with schemas to produce Spark DataFrames
# (dim_customers, dim_products, txns_df).

# DRY_RUN guard: skip writes unless explicitly allowed.

# Stop Spark: tidy closure.


# 🔁 Full syntax breakdown (key items)

# sc.parallelize(range(...), PARTITIONS) → create an RDD of ints with specified partitions.

# mapPartitions(func) → run func once per partition with an iterator of inputs; best for initializing Faker 
# once per partition.

# spark.createDataFrame(rdd, schema) → convert RDD of tuples into typed DataFrame.

# df.write.partitionBy(...).parquet(path) → write Parquet partitioned by columns (faster reads later).

# F.year("txn_ts") and F.month("txn_ts") used in partitionBy in code example are illustrative — in the writer 
# I used aliasing inside partitionBy; when persisting in production you may want to add columns year, month first.

# 🧠 Real-life analogy

# Think of Spark like a bakery: parallelize(range(...)) are the prepped dough trays, mapPartitions are ovens 
# where each oven (executor) bakes many rolls at once (generates many rows) — efficient vs baking one roll in 
# the main kitchen (driver).


# 🪞 Visual imagination

# Imagine a conveyor with 8 lanes (PARTITIONS). Each lane produces customers/products/transactions 
# independently and in parallel; no single central worker has to hold the whole dataset.

# 🧾 Summary table
# Component	Purpose	Benefits
# parallelize(..., PARTITIONS)	create workload chunks	Controls parallelism
# mapPartitions(gen_...)	generate rows per partition	Low memory footprint
# spark.createDataFrame(...)	typed DataFrame creation	Schema + Spark optimizations
# DRY_RUN flag	prevent writes	Safe testing
# 📦 Disk / size guidance (estimates — approximate)
# Typical transaction row: txn_id, timestamp, ints, channel — ~150–300 bytes depending on strings.

# 1M rows ≈ 150–300 MB raw (uncompressed); Parquet compression (snappy) often reduces to ~50–120 MB.

# Customers (50k) and products (5k) are small (tens of MB at most).

# Always leave > 3× expected output free to avoid spills.


# 🧪 Quick Practice Questions + Answers

# Q1. Why use mapPartitions() instead of map() for this generator?
# A1. mapPartitions() initializes Faker once per partition and yields many rows, which is more efficient
# than calling Faker() for every row inside map().

# Q2. What does parallelize(range(...), PARTITIONS) control?
# A2. It controls how many partitions (parallel tasks) the RDD will have — influences parallelism and memory 
# per task.

# Q3. If I want 5 million transactions but low RAM, what should I change?
# A3. Keep DRY_RUN=True while testing; increase PARTITIONS to split generation into more parallel, smaller tasks;
# write directly to Parquet (avoid collecting). Also run on a machine with more disk and memory or 
# reduce per-partition data.


# Next Step : > Silver → Gold ETL script that reads the Parquet (or the in-memory txns_df, dim_* frames if DRY_RUN True) and performs the enrichments, joins, aggregates, caching and final
# Parquet writes
# This ETL script assumes we have already generated the Bronze Parquet files.It’s safe-by-default (DRY_RUN=True) and will not write unless  explicitly allowed.


# 🔧 What this ETL does (high level)

# Read Bronze data (transactions, customers, products) from Parquet (or use in-memory DataFrames if they exist).

# Clean and enrich transactions (join with dims, derive total_amount, order_date, yyyymm).

# Optimize: broadcast joins on small dims, repartition by yyyymm, region, cache silver table.

# Build Gold outputs:

# monthly revenue by region & category

# top N customers per region (window + dense_rank)

# monthly revenue trend with 3-month moving average

# top products by revenue

# (Optional) Persist Gold tables as Parquet (or Delta if enabled).

# Register SQL views for ad-hoc queries and print sample outputs.



# ✅ Things to do before running
# Set INPUT_BASE below to match that OUTPUT_BASE.

# Make sure pyspark and faker installed (generator required faker; this ETL doesn’t need faker).

# Start with DRY_RUN = True to validate without writes.


# ETL Script:

"""
etl_project2.py
ETL (Silver -> Gold) for Project 2 (Big Data Retail Analytics)
- Safe defaults: DRY_RUN = True (no writes)
- Reads Bronze Parquet under INPUT_BASE (if it exists) OR expects the DataFrames to be present in the Spark session.
"""

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.functions import broadcast, col
from pyspark import StorageLevel
import os, sys, time

# ------------------ CONFIG ------------------
INPUT_BASE  = "out/project2_bronze"   # path where generator wrote txns, dim_customers, dim_products
OUTPUT_BASE = "out/project2_gold"     # where gold outputs are written (only if DRY_RUN=False)
DRY_RUN     = True                    # True => do not write outputs
USE_DELTA   = False                   # True => write Delta (requires delta-spark),
SHUFFLE_PARTITIONS = 16               # tune according to your machine/cluster cores
TOP_N_CUSTOMERS = 10                  # top-N per region


# ------------------ SPARK SESSION ------------------
spark = SparkSession.builder \
    .appName("Project2_ETL_Silver_to_Gold") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS)) \
    .getOrCreate()

print("Spark version:", spark.version)
t_start = time.perf_counter()


# ------------------ HELPER: read either parquet or raise friendly message ------------------
def read_if_exists(path, desc):
    if os.path.exists(path):
        print(f"Reading {desc} from {path} ...")
        return spark.read.parquet(path)
    else:
        print(f"WARNING: {desc} not found at {path}")
        return None
    
# ------------------ STEP 1: Read Bronze data (txns, customers, products) ------------------
# Try common expected paths: INPUT_BASE/txns, INPUT_BASE/dim_customers, INPUT_BASE/dim_products
txn_path = os.path.join(INPUT_BASE, "txns")
cust_path = os.path.join(INPUT_BASE, "dim_customers")
prod_path = os.path.join(INPUT_BASE, "dim_products")

txns_df = read_if_exists(txn_path, "Transactions")
dim_customers = read_if_exists(cust_path, "Customers")
dim_products = read_if_exists(prod_path, "Products")


# If NONE of these exist, inform user and stop (we can't proceed) 
if txns_df is None and dim_customers is None and dim_products is None:
    print("\nERROR: Bronze datasets not found. Options:")
    print(" * Run the generator script with DRY_RUN=False and set INPUT_BASE accordingly ") 
    print(" * Or, if you generated in the same interactive Spark session, attach the DataFrames with those names")
    print("Exiting ETL.")
    spark.stop()
    sys.exit(1)
    
# Basic schema check + show samples (safe)
print("\n--- txns sample ---")
txns_df.printSchema()
txns_df.show(3, truncate=False)
print("\n--- customers sample ---")
dim_customers.printSchema()
dim_customers.show(3, truncate=False)
print("\n--- products sample ---")
dim_products.printSchema()
dim_products.show(3, truncate=False)


# ------------------ STEP 2: Clean & Enrich (Silver) ------------------
# Why: get analytical columns, remove bad rows, prepare partitioning keys
# What you do: broadcast join small dims, compute total_amount, yyyymm; remove invalid rows; repartition; cache

# Broadcast dims (safe if dims are small)
dim_customers_small = dim_customers.select("customer_id", "region", "gender")
dim_products_small  = dim_products.select("product_id", "category", "subcategory", "base_price")

# Join: use broadcast to avoid shuffling the large txns table
silver = (
    txns_df
    .join(broadcast(dim_customers_small), on="customer_id", how='left')
    .join(broadcast(dim_products_small), on="product_id", how='left')
    .withColumn("unit_price", F.coalesce(col("base_price"), F.lit(0.0)))   # if price missing => 0
    .withColumn("total_amount", col("unit_price") * col("quantity"))
    .withColumn("order_date", F.to_date("txn_ts"))
    .withColumn("yyyymm", F.date_format("txn_ts", "yyyyMM"))
)


# Cleaning rules: drop txns with zero/negative qty or pric
silver = silver.filter((col("quantity") > 0) & (col("unit_price") > 0))

# Deduplicate by txn_id (idempotency)
silver = silver.dropDuplicates(["txn_id"])

# Repartition by keys commonly used in queries (improves downstream shuffles & read pruning)
silver = silver.repartition(SHUFFLE_PARTITIONS, col("yyyymm"), col("region"))

# Cache silver if we will use it multiple times (we will)
silver.persist(StorageLevel.MEMORY_AND_DISK)
silver_count = silver.count()  # trigger action to cache
print(f"\nSilver rows: {silver_count} | partitions: {silver.rdd.getNumPartitions()}")


# ------------------ STEP 3: GOLD aggregates ------------------
# 3.1 Monthly revenue by region & category
gold_monthly_region_cat = (
    silver.groupBy("yyyymm", "region", "category")
    .agg(
        F.sum("total_amount").alias("revenue"),
        F.sum("quantity").alias("units_sold")
    )
    .orderBy("yyyymm", "region", "category")
)

# 3.2 Top N customers per region (window + dense_rank)
cust_spend = silver.groupBy("region", "customer_id").agg(F.sum("total_amount").alias("total_spent"))
win = Window.partitionBy("region").orderBy(F.desc("total_spent"))
gold_top_customers = (
    cust_spend
    .withColumn("rank", F.dense_rank().over(win))
    .filter(col("rank") <= TOP_N_CUSTOMERS)
    .orderBy("region", "rank")
)

# 3.3 Monthly revenue trend + 3-month moving average
monthly_total = silver.groupBy("yyyymm").agg(F.sum("total_amount").alias("revenue"))
win_ma = Window.orderBy("yyyymm").rowsBetween(-2, 0)  # current + 2 previous = 3-month window
gold_rev_trend = monthly_total.withColumn("rev_ma_3m", F.avg("revenue").over(win_ma)).orderBy("yyyymm")

# 3.4 Top products by revenue
gold_top_products = (
    silver.groupBy("product_id", "product_name", "category")
    .agg(F.sum("total_amount").alias("revenue"), F.sum("quantity").alias("units"))
    .orderBy(F.desc("revenue"))
    .limit(50)
)

# ------------------ STEP 4: Persist Gold outputs (guarded by DRY_RUN) ------------------
if not DRY_RUN:
    print(f"\nPersisting Gold outputs to {OUTPUT_BASE} ...")
    if USE_DELTA:
        # requires delta-spark installation and builder config
        gold_monthly_region_cat.write.format("delta").mode("overwrite").save(os.path.join(OUTPUT_BASE, "gold_monthly_region_cat"))
        gold_top_customers.write.format("delta").mode("overwrite").save(os.path.join(OUTPUT_BASE, "gold_top_customers"))
        gold_rev_trend.write.format("delta").mode("overwrite").save(os.path.join(OUTPUT_BASE, "gold_rev_trend"))
        gold_top_products.write.format("delta").mode("overwrite").save(os.path.join(OUTPUT_BASE, "gold_top_products"))
    else:
        gold_monthly_region_cat.write.mode("overwrite").parquet(os.path.join(OUTPUT_BASE, "gold_monthly_region_cat"))
        gold_top_customers.write.mode("overwrite").parquet(os.path.join(OUTPUT_BASE, "gold_top_customers"))
        gold_rev_trend.write.mode("overwrite").parquet(os.path.join(OUTPUT_BASE, "gold_rev_trend"))
        gold_top_products.write.mode("overwrite").parquet(os.path.join(OUTPUT_BASE, "gold_top_products"))
    print("Persist complete.")
else:
    print("\nDRY_RUN=True => no writes. Previewing outputs with .show() (safe)")


# ------------------ STEP 5: Register SQL views & quick verification ------------------
silver.createOrReplaceTempView("silver_orders")
gold_monthly_region_cat.createOrReplaceTempView("gold_monthly_region_cat")
gold_top_customers.createOrReplaceTempView("gold_top_customers")
gold_rev_trend.createOrReplaceTempView("gold_rev_trend")
gold_top_products.createOrReplaceTempView("gold_top_products")


print("\n=== GOLD: monthly revenue by region & category (sample) ===")
gold_monthly_region_cat.show(8, truncate=False)

print("\n=== GOLD: Top customers per region (sample) ===")
gold_top_customers.show(8, truncate=False)

print("\n=== GOLD: Revenue trend with 3-month MA (sample) ===")
gold_rev_trend.show(8, truncate=False)

print("\n=== GOLD: Top products by revenue (sample) ===")
gold_top_products.show(8, truncate=False)

# ------------------ STEP 6: Cleanup ------------------
# Unpersist silver when done to free RAM
silver.unpersist()
t_end = time.perf_counter()
print(f"\nETL completed in {(t_end - t_start):.2f} seconds (wall-clock)")

spark.stop()

# 🔍 Why broadcast joins here?

# txns (fact) can be very large. Joining it with small dimension tables (customers/products) by broadcasting dims avoids shuffling the entire fact dataset across the network. That turns an expensive distributed join into a map-side lookup — much faster.

# 🧱 What the repartition(SHUFFLE_PARTITIONS, "yyyymm", "region") does

# Physically re-distributes rows across partitions using (yyyymm, region) as keys.

# Helps subsequent groupBy operations (which often use yyyymm/region) to have less cross-node data movement.

# Tuning SHUFFLE_PARTITIONS depends on CPU cores & data size; default 200 is often too high for laptops.

# 💬 Inline comments built into script

# Each block contains a comment explaining purpose and risks (e.g., clean rules, caching strategy).


# Each block contains a comment explaining purpose and risks (e.g., clean rules, caching strategy).

# 🔁 Full syntax breakdown of key functions

# broadcast(df) — send df to all executors.

# withColumn("yyyymm", date_format("txn_ts","yyyyMM")) — derives partitionable month key.

# persist(MEMORY_AND_DISK) — store DataFrame in memory, spill to disk if needed.

# Window.partitionBy(...).orderBy(...) — window specification for rank/MA.

# 🧠 Real-life analogy

# Think of silver as a staging table on the warehouse floor with items pre-sorted by month & region. Workers 
# (tasks) can now easily compute monthly stacks (aggregates) without moving boxes across the warehouse.

# 🪞 Visual Imagination

# Visualize the dataset as boxes on multiple conveyor belts. Broadcast joins are small labels pasted to each
# box locally; repartition groups belts by region/month; caching is leaving a basket of frequently accessed boxes at hand.


# 🧾 Summary Table
# Step	Purpose	Spark primitives used
# Read Bronze	Load raw parquet	spark.read.parquet
# Enrich	Join dims, compute metrics	join(broadcast(...)), withColumn
# Clean	Remove invalid rows	filter, dropDuplicates
# Partition & Cache	Improve parallelism & reuse	repartition, persist
# Gold Aggr	Business KPIs	groupBy, Window, agg
# Persist	Save final tables	.write.parquet() / delta


# 🧪 Practice Questions + Answers

# Q1. Why do we persist() silver before computing multiple gold outputs?
# A1. Because silver is reused for multiple aggregations; persisting avoids recomputing expensive joins/derivations for each aggregation.

# Q2. What would happen if we didn’t broadcast dims in the join?
# A2. Spark would perform a shuffle join — likely moving huge amounts of data across network and causing slower performance and higher shuffle I/O.

# Q3. How can you detect that broadcast join was used after running this ETL?
# A3. Check Spark UI → SQL tab or Stage details: look for BroadcastHashJoin in the physical plan, and small shuffle read for dims.

# Q4. Why do we filter out unit_price <= 0 and quantity <= 0?
# A4. To remove invalid or corrupted rows that would skew revenue/aggregation metrics.


# ✅ Next steps (recommended)

# Run the generator with DRY_RUN=False (if you haven’t) to produce Bronze Parquet under INPUT_BASE.

# Run this ETL with DRY_RUN=True first to inspect data & samples.

# Toggle DRY_RUN=False to persist Gold outputs; inspect Parquet sizes and file counts.

# Open Spark UI (http://localhost:4040) during runs to validate:

# BroadcastHashJoin usage

# Shuffle read/write sizes

# Silver persisted in Storage tab

