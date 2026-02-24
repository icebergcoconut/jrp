import json
import os

def create_markdown_cell(source):
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + "\n" if i < len(source.split('\n')) - 1 else line for i, line in enumerate(source.split('\n'))]
    }

def create_code_cell(source):
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [line + "\n" if i < len(source.split('\n')) - 1 else line for i, line in enumerate(source.split('\n'))]
    }

# ==========================================
# 1. BLAZINGMQ NOTEBOOK
# ==========================================
bmq_cells = []

bmq_cells.append(create_markdown_cell("""# 🚀 Deep Dive: Why BlazingMQ? (Message Queues vs Direct Integration)

In this notebook, we'll explore why we placed **BlazingMQ** between our Data Fetcher (Producer) and our Databricks Data Lake (Consumer). 

While you may have used Kafka or RabbitMQ before, the fundamental question to answer when designing such a system is: **Why use a Message Queue at all?** Why not just have the fetcher save directly to the database?

### Core Concepts You Will Learn:
1. **The 'Slow Consumer' Problem**: What happens without a message queue.
2. **Asynchronous Buffer**: How an MQ decouples producers and consumers.
3. **BlazingMQ Specifics**: Why Bloomberg built this for financial market data."""))

bmq_cells.append(create_code_cell("""# Run this first to ensure necessary libraries are installed
!pip install pandas"""))

bmq_cells.append(create_markdown_cell("""## Scenario 1: Direct Integration (No Message Queue)

Imagine the stock market opens and thousands of ticker price updates flow in per second. Your Python fetcher is downloading them instantly. But, writing them to a massive Delta Lake Database requires network calls, data validation, and heavy disk I/O.

Let's simulate this. The Consumer takes 0.5 seconds to process one data point. Watch how it immediately forms a bottleneck and slows down the whole application."""))

bmq_cells.append(create_code_cell("""import time
import random

# Simulating 5 rapid data ticks fetched from the market
market_ticks = [
    {"ticker": "7203.T", "price": 2800.0, "time": "09:00:01"},
    {"ticker": "7203.T", "price": 2801.5, "time": "09:00:01"},
    {"ticker": "7203.T", "price": 2802.0, "time": "09:00:02"},
    {"ticker": "7203.T", "price": 2799.0, "time": "09:00:02"},
    {"ticker": "7203.T", "price": 2805.0, "time": "09:00:03"}
]

def slow_database_write(tick):
    '''Simulates a heavy database insertion.'''
    time.sleep(0.5) 
    print(f"[{time.strftime('%H:%M:%S')}] CONSUMER: Successfully saved {tick['ticker']} at {tick['price']}")

print("--- Starting Direct API Integration (Blocking) ---")
start_time = time.time()

# The Producer is forced to wait for every single consumer write!
for tick in market_ticks:
    print(f"[{time.strftime('%H:%M:%S')}] PRODUCER: Fetched new data: {tick['price']}. Sending to database...")
    slow_database_write(tick)

print(f"--- Finished in {time.time() - start_time:.2f} seconds ---")"""))

bmq_cells.append(create_markdown_cell("""### ❌ The Problem: 
The Producer (yfinance fetcher) is completely blocked. It took over 2.5 seconds just to handle 5 ticks. If a million ticks came in, your fetcher would crash, timeout, or miss live prices entirely because it is permanently stuck waiting for the database to catch up.

---

## Scenario 2: The BlazingMQ Architecture (Decoupled & Asynchronous)

By placing BlazingMQ in the middle, the Producer doesn't talk to the database anymore. It just dumps the message into BlazingMQ's in-memory queue. BlazingMQ confirms receipt almost instantly. Then, the Consumer can slowly drain the queue at its own safe pace.

Let's simulate the buffer!"""))

bmq_cells.append(create_code_cell("""import queue
import threading

# Simulating BlazingMQ in-memory buffer
blazing_mq = queue.Queue()

def blazingmq_producer():
    start_time = time.time()
    for tick in market_ticks:
        blazing_mq.put(tick)
        print(f"[{time.strftime('%H:%M:%S')}] PRODUCER: Instantly queued data: {tick['price']}")
        time.sleep(0.01) # Ultra-fast market fetch
        
    print(f"✅ PRODUCER: Finished all my work in {time.time() - start_time:.2f} seconds. I am free to do other things!")

def blazingmq_consumer():
    while not blazing_mq.empty():
        tick = blazing_mq.get()
        slow_database_write(tick) # The slow 0.5s write
        blazing_mq.task_done()
    print("✅ CONSUMER: Automatically finished draining the queue!")

print("--- Starting Decoupled MQ Integration ---")

# We run them concurrently to represent two separate microservices
producer_thread = threading.Thread(target=blazingmq_producer)
consumer_thread = threading.Thread(target=blazingmq_consumer)

producer_thread.start()
time.sleep(0.1) # Let producer finish immediately
consumer_thread.start()

producer_thread.join()
consumer_thread.join()"""))

bmq_cells.append(create_markdown_cell("""### ✅ The Solution:
Notice what happened?! The Producer dumped all 5 ticks in **0.05 seconds** and was "free". It didn't care that the Consumer was grinding away taking 2.5 seconds. 

### 💡 Why BlazingMQ, specifically?
The reasons for selecting Bloomberg's BlazingMQ over alternatives include:

1. **Deterministic Low Tail-Latency**: Financial data loses value by the millisecond. RabbitMQ pauses for garbage collection and Kafka requires partition rebalancing. BlazingMQ guarantees low, predictable latency without these massive pauses, preventing "stale" ticks from piling up.
2. **Work-Stealing vs Partitioning**: Kafka ties a consumer to a specific partition. If that consumer crashes, the whole partition pauses. BlazingMQ uses a 'peer-to-peer' mesh network. Any consumer can grab the next message instantly.
3. **Fail-safe Multi-Tenancy**: Built for Bloomberg, it ensures that if one consumer handles Toyota stock, and another handles Sony, a massive spike in Toyota volume won't crash the Sony feeds."""))

bmq_notebook = {
    "cells": bmq_cells,
    "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
    "nbformat": 4,   "nbformat_minor": 4
}


# ==========================================
# 2. DATABRICKS / SPARK NOTEBOOK
# ==========================================
dbx_cells = []

dbx_cells.append(create_markdown_cell("""# 🧠 Deep Dive: Databricks, Apache Spark, and Delta Lake

In this notebook, we explore why big data architectures don't use PostgreSQL or standard CSV files for feature engineering and quantitative analysis.

### Core Concepts You Will Learn:
1. **Vertical Scaling (Postgres) vs Horizontal Scaling (Spark)**
2. **What makes Apache Spark so powerful?**
3. **Data Lakes vs Delta Lakes**: Fixing the "Corrupted File" problem with ACID transactions."""))

dbx_cells.append(create_code_cell("""# Setup dependencies (PySpark requires JVM internally, but can run locally in Python)
!pip install pyspark pandas"""))

dbx_cells.append(create_markdown_cell("""## 1. Relational Databases (PostgreSQL) vs Apache Spark

### 💡 The Problem with PostgreSQL
PostgreSQL is a massive single computer (Vertical Scaling). If you want to compute the 25-day Moving Average of every stock in the S&P 500 spanning 20 years, an RDBMS has to scan rows sequentially on one database server's single hard drive. Eventually, it runs out of RAM or CPU and grinds to a halt.

### ✅ The Solution: Databricks / Apache Spark
Databricks orchestrates **Apache Spark**. Spark splits your dataset into a thousand smaller chunks across hundreds of separate computers (Horizontal Scaling). It runs the Moving Average math simultaneously in RAM across all nodes, and stitches the answer back together in seconds.

Let's execute actual PySpark code to see how it looks identical to standard code, but operates on distributed architecture!"""))

dbx_cells.append(create_code_cell("""from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg

# Initialize a local Spark "Cluster" inside this notebook
print("Spinning up local Apache Spark cluster...")
spark = SparkSession.builder.appName("TradingBacktestApp").getOrCreate()

# Create dummy stock data spreading across our DataFrame
data = [
    ("7203.T", "2024-01-01", 2000.0),
    ("7203.T", "2024-01-02", 2050.0),
    ("7203.T", "2024-01-03", 2100.0),
    ("7203.T", "2024-01-04", 1950.0),
    ("7203.T", "2024-01-05", 2010.0),
]
df_spark = spark.createDataFrame(data, ["Ticker", "Date", "Close"])

print("\\nRaw Spark Distributed DataFrame:")
df_spark.show()"""))

dbx_cells.append(create_markdown_cell("""### Calculating Distributed Features
Notice how we define a "Window". In Spark, data is scattered across machines. A `Window.partitionBy("Ticker")` tells Spark to ensure all "7203.T" stock records get sent to the exact same computing node, so the moving average is calculated correctly without dragging data across the network."""))

dbx_cells.append(create_code_cell("""# Distribute the data by Ticker, sort by Date
windowSpec = Window.partitionBy("Ticker").orderBy("Date").rowsBetween(-2, 0) # 3-day MA for simplicity

# Calculate average across the window
df_features = df_spark.withColumn("SMA_3", avg(col("Close")).over(windowSpec))

print("Feature Engineered Distributed DataFrame:")
df_features.show()"""))

dbx_cells.append(create_markdown_cell("""## 2. Standard Data Lakes (CSV/Parquet) vs Delta Lake

So far, we have high performance computation using Spark. Now we need to save the data permanently to storage like AWS S3 or Azure Blob.

### ❌ The "Standard CSV" Problem
In a normal Data Lake, you save a giant `.csv` or `.parquet` file. But what if the cluster crashes right in the middle of writing the CSV?
You are left with half a file. The next person who reads the data gets corrupted, broken CSV data, and a crashed application.

Let's simulate a corrupted write."""))

dbx_cells.append(create_code_cell("""import os
import time

filename_csv = "corrupt_data.csv"

def simulate_crashing_csv_write():
    print("Writing 1,000,000 rows to CSV...")
    with open(filename_csv, "w") as f:
        f.write("Ticker,Date,Close\\n")
        f.write("7203.T,2024-01-01,2000.0\\n")
        f.write("7203.T,2024-01-02,2050.0\\n")
        # --- CRASH SIMULATION ---
        print("💥 CRITICAL SERVER ERROR: Out of Memory! Crash!")
        f.write("7203.T,2") # File cuts off mid-write!

simulate_crashing_csv_write()

print("\\nTrying to read the corrupted CSV:")
with open(filename_csv, "r") as f:
    print(f.read())"""))

dbx_cells.append(create_markdown_cell("""### ✅ The Solution: Delta Lake ACID Transactions

Databricks invented **Delta Lake**. Instead of just writing a file, it writes into a highly structured transaction log.

If Delta Lake crashes mid-write, it completely rolls back the failed transaction, acting exactly as a mature Postgres database would. The user trying to read the data will simply see the old, clean version of the data until the new data safely commits 100%.

Furthermore, Delta Lake enables **Time Travel**. Because it logs everything, you can literally run SQL like:
`SELECT * FROM my_table TIMESTAMP AS OF '2023-01-01'` and get the exact snapshot of data that existed on that day! This is critical for ML models to prevent look-ahead bias and rebuild old models.

### 🎙️ Architectural Defense
**Question:** "Why use Databricks Delta Lake instead of saving to a Postgres RDS database?"

**Answer:** "Postgres scales vertically. It is fundamentally impossible to calculate 20-year rolling window functions across 5,000 stocks on a single database efficiently. I used Databricks because Apache Spark scales horizontally across hundreds of nodes to slice feature engineering times from hours to seconds. However, traditional Data Lakes (S3 buckets of Parquet files) lack ACID transactions, meaning pipeline crashes corrupt data. Delta Lake bridges this gap, providing horizontal massive-scale computing paired with the safety of atomic rollback transactions and Time Travel versioning." """))


dbx_notebook = {
    "cells": dbx_cells,
    "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"}},
    "nbformat": 4,   "nbformat_minor": 4
}


# ==========================================
# WRITE BOTH FILES
# ==========================================
os.makedirs("notebooks", exist_ok=True)

with open("notebooks/BlazingMQ_Deep_Dive.ipynb", "w", encoding='utf-8') as f:
    json.dump(bmq_notebook, f, indent=2)

with open("notebooks/Databricks_Spark_Deep_Dive.ipynb", "w", encoding='utf-8') as f:
    json.dump(dbx_notebook, f, indent=2)

print("Both notebooks successfully generated!")
