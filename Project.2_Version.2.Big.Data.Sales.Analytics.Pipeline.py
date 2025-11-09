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

