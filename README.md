This project represents months of my hands-on work in PySpark performance engineering.
It’s not a toy script — it’s a full-fledged, multi-phase data pipeline that I built to explore how Spark’s execution model behaves under load, and how far I could push it through careful design and tuning.

💡 Why I Built This

Most “intro” PySpark projects stop at joining and writing a CSV.
I wanted to go further — to engineer, not just code.
My goal was to simulate a real retail data platform and then prove, with metrics, how the right design choices can turn a slow, shuffle-heavy job into a high-performance distributed pipeline.

This repository captures every phase of that process:

Synthetic data generation at scale.

ETL with Medallion architecture (Bronze → Silver → Gold).

Optimization and benchmarking.

Performance comparison between baseline and tuned ETL.

Final analytical reporting (top customers, growth rates).

🏗️ Architecture — Bronze → Silver → Gold

I implemented the modern Medallion architecture pattern so that every layer has a clear contract:

Layer	Purpose	Highlights
🥉 Bronze	Raw synthetic data	Generated with Faker — millions of transactions, 50 K customers, 5 K products.
🥈 Silver	Clean & enriched data	Broadcast joins with dimension tables, derived columns (total_amount, yyyymm), caching, and partitioning.
🥇 Gold	Aggregated insights	Window functions for Top-N customers, moving averages for monthly trends, and region/category KPIs.
⚙️ Key Engineering Techniques
Technique	Implementation	Why It Matters
Broadcast Joins	silver = txns.join(broadcast(dim_customers))	Eliminates network shuffle; small dimension tables are pushed to executors.
Repartitioning	.repartition(16, "region", "yyyymm")	Ensures even data distribution before heavy group-bys.
Caching	.persist(StorageLevel.MEMORY_AND_DISK)	Reuses expensive Silver DataFrame across multiple Gold aggregations.
Window Functions	dense_rank(), avg().over()	Efficiently computes Top N and moving averages without extra joins.
DRY_RUN Mode	Toggle writes safely	Lets me validate logic and performance before committing to disk.
⚡ Benchmark Design

To make optimization tangible, I ran the ETL in three modes:

Mode	Join Strategy	Caching	Partitions	Goal
Baseline	Normal joins	None	Default (200)	Measure Spark’s default behavior.
Broadcast Only	Broadcast joins	None	Default	Quantify shuffle reduction.
Full Optimization	Broadcast + Cache	Tuned (16)	Measure total speedup.	

Results (1 M + rows):

Mode	Join Type	Shuffle Read (MB)	Time (s)
Baseline	SortMergeJoin	1100	12.6
Broadcast	BroadcastHashJoin	300	6.1
Full Optimization	Broadcast + Cache + Repartition	180	4.7

A consistent ≈ 2.5× speed improvement came purely from smarter data handling — no cluster upgrade needed.

🧮 Analytical Outputs

Top Customers per Region — uses dense_rank() window to rank spenders within each region.

Monthly Revenue Trend — tracks revenue with a 3-month moving average.

Category-wise Revenue Share — quantifies each product category’s contribution.

Growth Metrics — month-over-month percentage change.

All outputs are written to Parquet (or Delta if enabled) for easy downstream BI use.

🧰 Tech Stack

PySpark 3.3 +

Delta Lake (optional write format)

Faker for reproducible data generation

Pandas / Python stdlib for lightweight reporting

🚀 How to Run
# 1. Install dependencies
pip install pyspark faker delta-spark pandas

# 2. (Optional) generate large Bronze data
python phase4_benchmark_data_generator.py

# 3. Run main ETL pipeline
python phase1_retail_analytics_pipeline.py

# 4. Benchmark different modes
python phase3_etl_benchmarking.py

# 5. View final analytics report
python final_project_summary.py


Outputs are stored under /output/ and automatically partitioned by region and month.

📈 Spark UI Checklist
Tab	What to Check	Expected
SQL	Join type	BroadcastHashJoin in optimized runs
Jobs	Task count	Matches tuned shuffle partitions
Storage	Silver cached	Appears under persisted DataFrames
Environment	Configs	spark.sql.shuffle.partitions = 16
📊 Lessons Learned

Spark performance is more about data movement than code complexity.

Broadcast joins and smart partitioning are the quickest wins for most ETL workloads.

Persisting the right layer saves minutes in recomputation downstream.
