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

cells = []

# Cell 1
cells.append(create_markdown_cell("""# 📈 JP Signal — Architecture Deep Dive & Interview Mastery Guide

Welcome to the interactive guide for the **Japan Stock Trading Signal App**.
This notebook is designed to train you to confidently explain, demonstrate, and defend the architectural choices of this project during senior engineering interviews.

### Core Objectives:
1. Trace the data flow from ingestion to inference.
2. Understand the rationale behind **BlazingMQ**, **Databricks**, and **SageMaker**.
3. Execute the core logic locally.
4. Master the Mock Interview Q&A."""))

# Cell 2
cells.append(create_markdown_cell("""## 1. Data Ingestion: yfinance to Message Queue

We begin by pulling raw market data. In production, this data stream from `yfinance` is published directly to a message broker."""))

# Cell 3: Environment Setup
cells.append(create_code_cell("""# Run this cell to ensure all dependencies are installed in your current kernel
!pip install yfinance pandas scikit-learn xgboost"""))

# Cell 4
cells.append(create_code_cell("""import yfinance as yf
import pandas as pd
import json

ticker = "7203.T"  # Toyota Motor
print(f"Fetching data for {ticker}...")

# Fetch 2 years of daily data
df = yf.download(ticker, start="2022-01-01", end="2024-01-01", progress=False)

# Clean up MultiIndex columns if present
if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.droplevel(1)

print(f"Successfully fetched {len(df)} days of OHLCV data.")
df.head(3)"""))

# Cell 4
cells.append(create_markdown_cell("""## 2. The Message Layer: Why BlazingMQ?

Once data is pulled, it is published to **BlazingMQ**.

### 💡 Interviewer: "Why did you choose BlazingMQ over Kafka or RabbitMQ?"

**The Master Answer:**
> "I chose BlazingMQ to demonstrate my familiarity with the Bloomberg ecosystem and modern high-performance messaging patterns. 
> 
> *   **vs Kafka:** Kafka is essentially an append-only distributed commit log. It requires partitioning for concurrency, which causes 'head-of-line' blocking if processing one partition is slow. BlazingMQ offers a simpler queue-centric peer-to-peer topology which natively supports work-stealing across consumers without rebalancing partitions.
> *   **vs RabbitMQ:** RabbitMQ uses a centralized broker pattern which becomes a bottleneck under high throughput. BlazingMQ was built by Bloomberg precisely to solve for multi-tenant, low-latency financial systems—it prioritizes deterministic low tail-latency and high throughput over traditional broker designs."

Let's simulate how the publisher sends this to the queue:"""))

# Cell 5
cells.append(create_code_cell("""def simulate_blazingmq_publisher(dataframe):
    messages = []
    for date, row in dataframe.iterrows():
        msg = {
            "ticker": ticker,
            "date": date.strftime("%Y-%m-%d"),
            "open": float(row.get('Open', 0)),
            "high": float(row.get('High', 0)),
            "low": float(row.get('Low', 0)),
            "close": float(row.get('Close', 0)),
            "volume": int(row.get('Volume', 0))
        }
        messages.append(msg)
    
    print(f"[BlazingMQ Publisher] Published {len(messages)} ticks to the exchange queue.")
    return messages

# Publish the last 3 days
mock_queue = simulate_blazingmq_publisher(df.tail(3))
print("\\nSample JSON Message on the Queue:")
print(json.dumps(mock_queue[-1], indent=2))"""))

# Cell 6
cells.append(create_markdown_cell("""## 3. The Data Platform: Databricks & Delta Lake

Consumers pull from BlazingMQ and write directly into **Delta Lake** on **Databricks**. 

### 💡 Interviewer: "What is the benefit of Databricks and Delta over just saving to a Postgres database?"

**The Master Answer:**
> "Financial feature engineering requires time-series window functions over massive datasets. Postgres scales vertically and struggles with huge analytical queries. 
> Databricks, built on Apache Spark, scales horizontally. More importantly, **Delta Lake** brings ACID transactions to object storage (like S3/Azure Blob). This means if our pipeline fails mid-write, we don't end up with corrupted historical data. Data is saved in open `Parquet` format, which is columnar and significantly faster for reading specific features into our machine learning models."

### Interactive: Databricks Credentials Config
If you wish to execute actual Databricks jobs, you can configure the environment here. Otherwise, the next cell will perform the exact same feature engineering using local `pandas`."""))

# Cell 7
cells.append(create_code_cell("""import os

print("=== Databricks Integration ===")
# In a real environment, you would use databricks-sdk
DATABRICKS_HOST = input("Enter Databricks Host URL (Leave empty for local simulation): ")
if not DATABRICKS_HOST:
    print("-> Using Local Pandas Simulation for Feature Engineering.")
else:
    DATABRICKS_TOKEN = input("Enter Databricks Token: ")
    os.environ["DATABRICKS_HOST"] = DATABRICKS_HOST
    os.environ["DATABRICKS_TOKEN"] = DATABRICKS_TOKEN
    print(f"-> Configured Databricks cluster at {DATABRICKS_HOST}")"""))

# Cell 8
cells.append(create_markdown_cell("""### 3.1 Feature Engineering (Databricks Simulation)
We calculate Simple Moving Averages (SMA), Relative Strength Index (RSI), and our ML Target (1 if future 20-day return > 3%)."""))

# Cell 9
cells.append(create_code_cell("""def compute_features_and_labels(data):
    df_feat = data.copy()
    
    # 1. Technical Indicators (SMA)
    df_feat['SMA_5'] = df_feat['Close'].rolling(window=5).mean()
    df_feat['SMA_25'] = df_feat['Close'].rolling(window=25).mean()
    
    # 2. RSI (14-day)
    delta = df_feat['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df_feat['RSI_14'] = 100 - (100 / (1 + rs))
    
    # 3. Label / Target (Future 20-day return > 3%)
    df_feat['Future_20d_Return'] = df_feat['Close'].shift(-20) / df_feat['Close'] - 1
    df_feat['Target'] = (df_feat['Future_20d_Return'] > 0.03).astype(int)
    
    # Drop NaNs created by rolling windows and shifts
    return df_feat.dropna()

featured_data = compute_features_and_labels(df)
print("[Databricks Simulation] Feature Engineering complete.")
featured_data[['Close', 'SMA_5', 'RSI_14', 'Future_20d_Return', 'Target']].tail()"""))

# Cell 10
cells.append(create_markdown_cell("""## 4. Machine Learning: AWS SageMaker & XGBoost

Once features are stored in Delta Lake, we train our model. In production, this training job runs on **AWS SageMaker**. 

### 💡 Interviewer: "Why serve the model via SageMaker rather than just loading the `.pkl` file in your FastAPI backend directly?"

**The Master Answer:**
> "Loading the model directly into the API (monolith approach) works for MVPs, but fails in production. 
> 1. **Resource Decoupling:** API servers are CPU-optimized for high concurrency workflows. ML Inference servers often benefit from different instance types (sometimes GPU/Inferentia).
> 2. **Independent Scaling:** If we get a spike in traffic, we can scale the FastAPI web pods independently from the heavier ML model endpoints.
> 3. **Model Versioning:** SageMaker lets us perform A/B testing or Shadow Deployments of new models without redeploying the backend codebase. The API simply calls a consistent SageMaker endpoint URL." """))

# Cell 11
cells.append(create_code_cell("""import xgboost as xgb
from sklearn.metrics import accuracy_score, classification_report
import warnings
warnings.filterwarnings('ignore')

# 1. Prepare Data
features = ['Close', 'SMA_5', 'SMA_25', 'RSI_14']
X = featured_data[features]
y = featured_data['Target']

# Time-series aware split (Strictly chronological to avoid look-ahead bias)
split_idx = int(len(X) * 0.8)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

print(f"Training on {len(X_train)} days, Testing on {len(X_test)} days...")

# 2. Train XGBoost Model (Simulating SageMaker Training Job)
model = xgb.XGBClassifier(
    n_estimators=100, 
    learning_rate=0.1, 
    max_depth=4,
    random_state=42
)
model.fit(X_train, y_train)

# 3. Evaluate Model
preds = model.predict(X_test)
print("\\n[SageMaker Simulation] Evaluation complete.")
print(f"Accuracy: {accuracy_score(y_test, preds):.2%}")
print("\\nClassification Report:\\n", classification_report(y_test, preds))"""))

# Cell 12
cells.append(create_markdown_cell("""## 5. Mock Interview: The Final Rapid-Fire Q&A

Review these strictly before the interview to solidify your architecture defense.

### Q: "Your app spans Azure (Web Apps), AWS (SageMaker), and Databricks. Isn't that overly complex?"
**A:** "For a simple MVP, yes. However, this is designed to demonstrate an enterprise-grade microservice architecture. Modern enterprises often use best-of-breed tools: Databricks is the undisputed leader for Spark data processing, SageMaker streamlines ML lifecycle management, and Azure offers excellent Docker hosting for the API. It proves I can design cloud-agnostic, event-driven systems that are tightly decoupled."

### Q: "How did you prevent look-ahead bias in your backtesting?"
**A:** "Two ways: First, in the ML train/test split, I used strictly chronological splitting rather than random cross-validation, ensuring the model never sees future data during training. Second, in the backtest engine, when a `BUY` signal is generated at the end of Day T (using closing prices), the engine executes the trade at the **Open price of Day T+1**, simulating realistic trade latency."

### Q: "If I wanted to add real-time streaming data instead of daily, what changes?"
**A:** "The architecture remains largely the same because we decoupled ingestion via **BlazingMQ**. We would update the yfinance producer to read a web-socket stream (e.g., from IBKR or Alpaca). Databricks would switch from batch notebook jobs to Spark Structured Streaming to incrementally update the Delta tables. SageMaker and FastAPI would continue to serve inferences exactly as they do now." """))

notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3"
        },
        "language_info": {
            "name": "python",
            "version": "3.11"
        }
    },
    "nbformat": 4,
    "nbformat_minor": 4
}

os.makedirs("notebooks", exist_ok=True)
with open("notebooks/Architecture_Deep_Dive.ipynb", "w", encoding='utf-8') as f:
    json.dump(notebook, f, indent=2)

print("Notebook generated successfully!")
