# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - 株価データ取得 (Data Ingestion)
# MAGIC Yahoo Financeから株価・財務データを取得し、テーブルに保存

# COMMAND ----------

# MAGIC %pip install yfinance

# COMMAND ----------

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# ── Local Execution Fallback ──
try:
    spark
except NameError:
    import os
    import shutil
    
    # Completely wipe local metastore and warehouse to force a fresh session
    for db_dir in ["spark-warehouse", "metastore_db"]:
        path = os.path.abspath(db_dir)
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
            
    # Fix for Java 21+ / Hadoop SecurityManager error
    os.environ["PYSPARK_SUBMIT_ARGS"] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'
    
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("LocalDataIngestion") \
        .config("spark.sql.warehouse.dir", os.path.abspath("spark-warehouse")) \
        .getOrCreate()
        
    # Mock the display function for local terminal
    def display(df):
        df.show()

# COMMAND ----------

# ── 対象銘柄 ──
TICKERS = [
    # 日本株 (東証)
    "7203.T",   # トヨタ自動車
    "6758.T",   # ソニーグループ
    "9984.T",   # ソフトバンクグループ
    "8306.T",   # 三菱UFJフィナンシャルG
    "6861.T",   # キーエンス
    # 米国株
    "AAPL",     # Apple
    "MSFT",     # Microsoft
    "GOOGL",    # Alphabet
    "AMZN",     # Amazon
    "NVDA",     # NVIDIA
    # ETF
    "SPY",      # S&P 500
    "QQQ",      # Nasdaq 100
    "VTI",      # Total Stock Market
]

print(f"対象銘柄数: {len(TICKERS)}")

# COMMAND ----------

# ── データ取得（過去2年分） ──
end_date = datetime.now()
start_date = end_date - timedelta(days=730)

all_data = []
errors = []

for ticker in TICKERS:
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)
        
        if hist.empty:
            print(f"⚠️  {ticker}: データなし")
            errors.append(ticker)
            continue
        
        hist = hist.reset_index()
        hist['ticker'] = ticker
        
        # Remove timezone BEFORE appending to all_data
        if 'Date' in hist.columns:
            hist['Date'] = pd.to_datetime(hist['Date'], utc=True).dt.tz_localize(None)
        
        # 財務データも取得
        info = stock.info
        hist['dividend_yield'] = info.get('dividendYield', 0) or 0
        hist['market_cap'] = info.get('marketCap', 0) or 0
        hist['pe_ratio'] = info.get('trailingPE', 0) or 0
        hist['sector'] = info.get('sector', 'Unknown')
        hist['company_name'] = info.get('shortName', ticker)
        
        all_data.append(hist)
        print(f"✅ {ticker} ({info.get('shortName', '?')}): {len(hist)} rows")
    except Exception as e:
        print(f"❌ {ticker}: {e}")
        errors.append(ticker)

print(f"\n取得完了: {len(all_data)}/{len(TICKERS)} 銘柄")
if errors:
    print(f"エラー銘柄: {errors}")

# COMMAND ----------

# ── データ結合 ──
df = pd.concat(all_data, ignore_index=True)

# 日付カラムの型変換は各銘柄ループ内で実施済み

print(f"合計レコード数: {len(df):,}")
print(f"銘柄数: {df['ticker'].nunique()}")
print(f"期間: {df['Date'].min().date()} ~ {df['Date'].max().date()}")
print(f"\nカラム: {df.columns.tolist()}")

# COMMAND ----------

# ── Spark DataFrameに変換して保存 ──
spark_df = spark.createDataFrame(df)

# テーブルとして保存
spark_df.write.mode("overwrite").saveAsTable("stock_prices_raw")

print("✅ stock_prices_raw テーブルに保存完了")
display(spark_df.limit(10))

# COMMAND ----------

# ── データ品質チェック ──
print("=== データ品質サマリー ===")
quality = spark.sql("""
    SELECT 
        ticker,
        company_name,
        COUNT(*) as row_count,
        MIN(Date) as first_date,
        MAX(Date) as last_date,
        ROUND(AVG(Close), 2) as avg_close,
        ROUND(MAX(dividend_yield) * 100, 2) as div_yield_pct,
        sector
    FROM stock_prices_raw
    GROUP BY ticker, company_name, sector
    ORDER BY ticker
""")
display(quality)
