# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - データエクスポート (Data Export)
# MAGIC 計算結果をCSV/JSONでエクスポート（Spring Boot バックエンドが読み込む用）

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Local Execution Fallback ──
try:
    spark
except NameError:
    import os
    # Fix for Java 21+ / Hadoop SecurityManager error
    os.environ["PYSPARK_SUBMIT_ARGS"] = '--driver-java-options "-Djava.security.manager=allow" pyspark-shell'
    
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("DataExport") \
        .config("spark.sql.warehouse.dir", os.path.abspath("spark-warehouse")) \
        .getOrCreate()
        
    # Mock the display function for local terminal
    def display(df):
        df.show()

# COMMAND ----------

# ── stock_signals テーブル読み込み ──
try:
    df = spark.table("stock_signals")
except Exception:
    import os
    print("⚠️ Local Table 'stock_signals' not found in catalog. Reading from Parquet...")
    df = spark.read.parquet(os.path.abspath("spark-warehouse/stock_signals"))
    
print(f"全レコード数: {df.count():,}")
print(f"銘柄数: {df.select('ticker').distinct().count()}")

# COMMAND ----------

# ── 最新データのみ抽出 ──
w = Window.partitionBy("ticker").orderBy(F.desc("Date"))
latest = (
    df.withColumn("rn", F.row_number().over(w))
      .filter(F.col("rn") == 1)
      .drop("rn")
)

print(f"最新レコード数: {latest.count()}")
display(
    latest.select(
        "ticker", "company_name", "Date", "Close",
        "sma_20", "sma_50", "rsi_14",
        "dividend_yield", "pe_ratio", "sector",
        "signal", "signal_strength", "dividend_category"
    ).orderBy("ticker")
)

# COMMAND ----------

# ── CSV出力 ──
# 全データ
full_pdf = df.toPandas()
full_pdf.to_csv("/tmp/stock_signals_full.csv", index=False)
print(f"✅ 全データCSV: /tmp/stock_signals_full.csv ({len(full_pdf):,} rows)")

# 最新データのみ
latest_pdf = latest.toPandas()
latest_pdf.to_csv("/tmp/latest_signals.csv", index=False)
print(f"✅ 最新データCSV: /tmp/latest_signals.csv ({len(latest_pdf)} rows)")

# COMMAND ----------

# ── JSON出力（API用） ──
import json

# 最新シグナル
latest_json = latest_pdf.to_json(orient="records", date_format="iso")
with open("/tmp/latest_signals.json", "w") as f:
    f.write(latest_json)
print("✅ 最新シグナルJSON: /tmp/latest_signals.json")

# 銘柄ごとの履歴（APIエンドポイント用）
for ticker in full_pdf['ticker'].unique():
    ticker_df = full_pdf[full_pdf['ticker'] == ticker].sort_values('Date')
    safe_name = ticker.replace(".", "_")
    ticker_df.to_json(f"/tmp/history_{safe_name}.json", orient="records", date_format="iso")

print(f"✅ 銘柄別履歴JSON: {full_pdf['ticker'].nunique()} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ダウンロード手順
# MAGIC 
# MAGIC 1. 上のセルを全て実行
# MAGIC 2. 以下のセルでファイルリストを確認
# MAGIC 3. Databricks UIの「File > Download」でダウンロード
# MAGIC    - または `dbutils.fs.cp("file:/tmp/xxx", "dbfs:/FileStore/xxx")` でFileStoreへコピー
# MAGIC    - FileStoreのURLは: `https://community.cloud.databricks.com/files/xxx`

# COMMAND ----------

# ── FileStoreにコピー（ダウンロードしやすくするため） ──
import os

export_files = [f for f in os.listdir("/tmp") if f.startswith(("stock_", "latest_", "history_"))]
print(f"エクスポートファイル数: {len(export_files)}")

for fname in sorted(export_files):
    src = f"file:/tmp/{fname}"
    dst = f"dbfs:/FileStore/rpm_export/{fname}"
    try:
        dbutils.fs.cp(src, dst)
        print(f"  📁 {fname} → FileStore")
    except NameError:
        print(f"  📁 {fname} → Local (Skipping dbutils upload)")

print(f"\n✅ FileStoreにコピー完了")
print(f"ダウンロードURL例:")
print(f"  https://community.cloud.databricks.com/files/rpm_export/latest_signals.csv")
print(f"  https://community.cloud.databricks.com/files/rpm_export/stock_signals_full.csv")

# COMMAND ----------

# ── エクスポートサマリー ──
print("=" * 60)
print("  📊 Research Platform Mini - データエクスポート完了")
print("=" * 60)
print(f"  期間: {full_pdf['Date'].min()} ~ {full_pdf['Date'].max()}")
print(f"  銘柄数: {full_pdf['ticker'].nunique()}")
print(f"  全レコード数: {len(full_pdf):,}")
print(f"  エクスポートファイル数: {len(export_files)}")
print()
print("  次のステップ:")
print("  1. CSVファイルをダウンロード")
print("  2. Spring Boot backendの resources/ に配置")
print("  3. DataIngestionService でロード")
print("=" * 60)
