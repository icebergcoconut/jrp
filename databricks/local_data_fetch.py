#!/usr/bin/env python3
"""
ローカルテスト用: Yahoo Financeからデータを取得してCSV/JSONに保存
Databricksを使わずにバックエンド開発・テストをする時に使用
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta

# ── 設定 ──
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "backend", "src", "main", "resources", "data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

TICKERS = [
    # 日本株
    "7203.T",   # トヨタ
    "6758.T",   # ソニー
    "9984.T",   # ソフトバンクG
    "8306.T",   # 三菱UFJFG
    "6861.T",   # キーエンス
    # 米国株
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    # ETF
    "SPY", "QQQ", "VTI",
]


def fetch_stock_data(tickers: list, days: int = 730) -> pd.DataFrame:
    """Yahoo Financeから株価データを取得"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    all_data = []
    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(start=start_date, end=end_date)
            if hist.empty:
                print(f"⚠️  {ticker}: データなし")
                continue

            hist = hist.reset_index()
            hist['ticker'] = ticker

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

    df = pd.concat(all_data, ignore_index=True)
    df['Date'] = pd.to_datetime(df['Date'], utc=True).dt.tz_localize(None)
    return df


def calculate_decisions(df: pd.DataFrame) -> pd.DataFrame:
    """テクニカル指標 & シグナル計算"""
    results = []

    for ticker in df['ticker'].unique():
        tdf = df[df['ticker'] == ticker].sort_values('Date').copy()

        # SMA
        tdf['sma_5'] = tdf['Close'].rolling(5).mean().round(2)
        tdf['sma_20'] = tdf['Close'].rolling(20).mean().round(2)
        tdf['sma_50'] = tdf['Close'].rolling(50).mean().round(2)
        tdf['sma_200'] = tdf['Close'].rolling(200).mean().round(2)

        # ゴールデンクロス / デッドクロス
        tdf['prev_sma_20'] = tdf['sma_20'].shift(1)
        tdf['prev_sma_50'] = tdf['sma_50'].shift(1)
        tdf['golden_cross'] = (tdf['sma_20'] > tdf['sma_50']) & (tdf['prev_sma_20'] <= tdf['prev_sma_50'])
        tdf['dead_cross'] = (tdf['sma_20'] < tdf['sma_50']) & (tdf['prev_sma_20'] >= tdf['prev_sma_50'])

        # RSI (14)
        delta = tdf['Close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = (-delta).where(delta < 0, 0)
        avg_gain = gain.rolling(14).mean()
        avg_loss = loss.rolling(14).mean()
        rs = avg_gain / avg_loss.replace(0, 0.001)
        tdf['rsi_14'] = (100 - (100 / (1 + rs))).round(2)

        # ボリンジャーバンド
        tdf['bb_upper'] = (tdf['sma_20'] + 2 * tdf['Close'].rolling(20).std()).round(2)
        tdf['bb_lower'] = (tdf['sma_20'] - 2 * tdf['Close'].rolling(20).std()).round(2)

        # リターン
        tdf['daily_return_pct'] = (tdf['Close'].pct_change() * 100).round(2)

        # 配当カテゴリ
        dy = tdf['dividend_yield'].iloc[-1] if len(tdf) > 0 else 0
        if dy >= 0.04:
            tdf['dividend_category'] = '高配当(4%+)'
        elif dy >= 0.02:
            tdf['dividend_category'] = '中配当(2-4%)'
        elif dy > 0:
            tdf['dividend_category'] = '低配当(<2%)'
        else:
            tdf['dividend_category'] = '無配当'

        # 総合シグナル
        conditions = [
            tdf['golden_cross'],
            tdf['dead_cross'],
            tdf['rsi_14'] < 30,
            tdf['rsi_14'] > 70,
            tdf['Close'] < tdf['bb_lower'],
            tdf['Close'] > tdf['bb_upper'],
        ]
        decisions = ['BUY', 'SELL', 'OVERSOLD', 'OVERBOUGHT', 'BUY_BB', 'SELL_BB']
        tdf['decision'] = np.select(conditions, decisions, default='HOLD')

        strength_map = {'BUY': 8, 'SELL': 8, 'OVERSOLD': 7, 'OVERBOUGHT': 7, 'BUY_BB': 6, 'SELL_BB': 6, 'HOLD': 3}
        tdf['decision_strength'] = tdf['decision'].map(strength_map)

        # 不要カラム削除
        tdf = tdf.drop(columns=['prev_sma_20', 'prev_sma_50'], errors='ignore')
        results.append(tdf)

    return pd.concat(results, ignore_index=True)


def export_data(df: pd.DataFrame, output_dir: str):
    """CSV/JSONでエクスポート"""
    # 全データCSV
    full_path = os.path.join(output_dir, "stock_backtests_full.csv")
    df.to_csv(full_path, index=False)
    print(f"📁 全データCSV: {full_path} ({len(df):,} rows)")

    # 最新データのみ
    latest = df.loc[df.groupby('ticker')['Date'].idxmax()]
    latest_path = os.path.join(output_dir, "latest_backtests.csv")
    latest.to_csv(latest_path, index=False)
    print(f"📁 最新データCSV: {latest_path} ({len(latest)} rows)")

    # JSON (API用)
    latest_json_path = os.path.join(output_dir, "latest_backtests.json")
    latest.to_json(latest_json_path, orient="records", date_format="iso", indent=2)
    print(f"📁 最新データJSON: {latest_json_path}")

    # 銘柄ごと
    history_dir = os.path.join(output_dir, "history")
    os.makedirs(history_dir, exist_ok=True)
    for ticker in df['ticker'].unique():
        safe = ticker.replace(".", "_")
        ticker_df = df[df['ticker'] == ticker].sort_values('Date')
        ticker_df.to_json(
            os.path.join(history_dir, f"{safe}.json"),
            orient="records", date_format="iso", indent=2
        )
    print(f"📁 銘柄別履歴JSON: {df['ticker'].nunique()} files → {history_dir}/")


def main():
    print("=" * 60)
    print("  📊 Research Platform Mini - ローカルデータ取得")
    print("=" * 60)

    print("\n[1/3] 株価データ取得中...")
    df = fetch_stock_data(TICKERS)
    print(f"\n合計: {len(df):,} rows, {df['ticker'].nunique()} tickers")

    print("\n[2/3] シグナル計算中...")
    decisions = calculate_decisions(df)

    print("\n[3/3] データエクスポート中...")
    export_data(decisions, OUTPUT_DIR)

    # サマリー表示
    latest = decisions.loc[decisions.groupby('ticker')['Date'].idxmax()]
    print("\n" + "=" * 60)
    print("  📊 最新シグナルサマリー")
    print("=" * 60)
    summary_cols = ['ticker', 'company_name', 'Close', 'sma_20', 'rsi_14', 'dividend_yield', 'decision']
    print(latest[summary_cols].to_string(index=False))
    print("=" * 60)
    print("✅ 完了！データは backend/src/main/resources/data/ に保存されました")


if __name__ == "__main__":
    main()
