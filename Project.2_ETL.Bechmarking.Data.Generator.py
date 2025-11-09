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