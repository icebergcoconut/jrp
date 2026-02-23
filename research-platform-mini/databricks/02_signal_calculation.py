# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - シグナル計算 (Signal Calculation)
# MAGIC テクニカル指標（SMA, RSI）とトレードシグナルを計算

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
    spark = SparkSession.builder.appName("SignalCalculation") \
        .config("spark.sql.warehouse.dir", os.path.abspath("spark-warehouse")) \
        .getOrCreate()
        
    # Mock the display function for local terminal
    def display(df):
        df.show()

# COMMAND ----------

# ── データ読み込み ──
try:
    df = spark.table("stock_prices_raw")
except Exception:
    import os
    print("⚠️ Local Table 'stock_prices_raw' not found in catalog. Reading from Parquet...")
    df = spark.read.parquet(os.path.abspath("spark-warehouse/stock_prices_raw"))
    
print(f"入力レコード数: {df.count():,}")

# COMMAND ----------

# ── ウィンドウ定義（銘柄ごとに日付順） ──
w = Window.partitionBy("ticker").orderBy("Date")

# ── 移動平均線 (SMA) ──
df = df.withColumn("sma_5",  F.round(F.avg("Close").over(w.rowsBetween(-4, 0)), 2))
df = df.withColumn("sma_20", F.round(F.avg("Close").over(w.rowsBetween(-19, 0)), 2))
df = df.withColumn("sma_50", F.round(F.avg("Close").over(w.rowsBetween(-49, 0)), 2))
df = df.withColumn("sma_200", F.round(F.avg("Close").over(w.rowsBetween(-199, 0)), 2))

print("✅ SMA(5, 20, 50, 200) 計算完了")

# COMMAND ----------

# ── ゴールデンクロス / デッドクロス ──
# SMA20がSMA50を上抜け → ゴールデンクロス（買いシグナル）
# SMA20がSMA50を下抜け → デッドクロス（売りシグナル）

df = df.withColumn("prev_sma_20", F.lag("sma_20", 1).over(w))
df = df.withColumn("prev_sma_50", F.lag("sma_50", 1).over(w))

df = df.withColumn("golden_cross",
    F.when(
        (F.col("sma_20") > F.col("sma_50")) & 
        (F.col("prev_sma_20") <= F.col("prev_sma_50")),
        F.lit(True)
    ).otherwise(F.lit(False))
)

df = df.withColumn("dead_cross",
    F.when(
        (F.col("sma_20") < F.col("sma_50")) & 
        (F.col("prev_sma_20") >= F.col("prev_sma_50")),
        F.lit(True)
    ).otherwise(F.lit(False))
)

print("✅ ゴールデンクロス / デッドクロス 計算完了")

# COMMAND ----------

# ── RSI (Relative Strength Index) 14日 ──
df = df.withColumn("price_change", F.col("Close") - F.lag("Close", 1).over(w))

df = df.withColumn("gain", 
    F.when(F.col("price_change") > 0, F.col("price_change")).otherwise(F.lit(0))
)
df = df.withColumn("loss", 
    F.when(F.col("price_change") < 0, F.abs(F.col("price_change"))).otherwise(F.lit(0))
)

df = df.withColumn("avg_gain", F.avg("gain").over(w.rowsBetween(-13, 0)))
df = df.withColumn("avg_loss", F.avg("loss").over(w.rowsBetween(-13, 0)))

df = df.withColumn("rsi_14",
    F.round(
        100 - (100 / (1 + F.col("avg_gain") / 
            F.when(F.col("avg_loss") == 0, F.lit(0.001)).otherwise(F.col("avg_loss"))
        )), 2
    )
)

print("✅ RSI(14) 計算完了")

# COMMAND ----------

# ── ボリンジャーバンド (20日) ──
df = df.withColumn("bb_std", 
    F.round(F.stddev("Close").over(w.rowsBetween(-19, 0)), 2)
)
df = df.withColumn("bb_upper", F.round(F.col("sma_20") + 2 * F.col("bb_std"), 2))
df = df.withColumn("bb_lower", F.round(F.col("sma_20") - 2 * F.col("bb_std"), 2))

print("✅ ボリンジャーバンド 計算完了")

# COMMAND ----------

# ── 日次リターン ──
df = df.withColumn("daily_return_pct",
    F.round((F.col("Close") - F.lag("Close", 1).over(w)) / F.lag("Close", 1).over(w) * 100, 2)
)

# ── 配当利回り分類 ──
df = df.withColumn("dividend_category",
    F.when(F.col("dividend_yield") >= 0.04, "高配当(4%+)")
     .when(F.col("dividend_yield") >= 0.02, "中配当(2-4%)")
     .when(F.col("dividend_yield") > 0, "低配当(<2%)")
     .otherwise("無配当")
)

print("✅ リターン・配当分類 計算完了")

# COMMAND ----------

# ── 総合シグナル ──
df = df.withColumn("signal",
    F.when(F.col("golden_cross") == True, F.lit("BUY"))
     .when(F.col("dead_cross") == True, F.lit("SELL"))
     .when(F.col("rsi_14") < 30, F.lit("OVERSOLD"))
     .when(F.col("rsi_14") > 70, F.lit("OVERBOUGHT"))
     .when(F.col("Close") < F.col("bb_lower"), F.lit("BUY_BB"))
     .when(F.col("Close") > F.col("bb_upper"), F.lit("SELL_BB"))
     .otherwise(F.lit("HOLD"))
)

# シグナル強度スコア (1-10)
df = df.withColumn("signal_strength",
    F.when(F.col("signal") == "BUY", F.lit(8))
     .when(F.col("signal") == "SELL", F.lit(8))
     .when(F.col("signal") == "OVERSOLD", F.lit(7))
     .when(F.col("signal") == "OVERBOUGHT", F.lit(7))
     .when(F.col("signal") == "BUY_BB", F.lit(6))
     .when(F.col("signal") == "SELL_BB", F.lit(6))
     .otherwise(F.lit(3))
)

print("✅ 総合シグナル計算完了")

# COMMAND ----------

# ── 不要カラム削除 & 保存 ──
result = df.drop(
    "prev_sma_20", "prev_sma_50", 
    "price_change", "gain", "loss", 
    "avg_gain", "avg_loss", "bb_std"
)

result.write.mode("overwrite").saveAsTable("stock_signals")

print("✅ stock_signals テーブルに保存完了")

# COMMAND ----------

# ── 結果プレビュー ──
print("=== 最新シグナル（直近20件） ===")
display(
    result.select(
        "ticker", "company_name", "Date", "Close", 
        "sma_20", "sma_50", "rsi_14", 
        "dividend_yield", "signal", "signal_strength"
    ).orderBy(F.desc("Date")).limit(20)
)

# COMMAND ----------

# ── シグナル分布 ──
print("=== シグナル分布 ===")
display(
    result.groupBy("signal")
    .agg(F.count("*").alias("count"))
    .orderBy(F.desc("count"))
)
