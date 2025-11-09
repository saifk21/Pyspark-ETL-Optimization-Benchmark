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




  
