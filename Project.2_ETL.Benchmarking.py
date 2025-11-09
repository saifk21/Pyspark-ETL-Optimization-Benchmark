
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

