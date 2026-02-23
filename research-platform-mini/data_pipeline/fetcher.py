"""
Yahoo Finance Data Fetcher
Downloads OHLCV + fundamental data for 10 Japanese stocks.
Outputs clean DataFrames ready for BlazingMQ producer or direct pipeline usage.
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Optional

from data_pipeline.config import TICKERS, TICKER_LIST, START_DATE, END_DATE, FUNDAMENTAL_METRICS, DATA_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def fetch_ohlcv(ticker: str, start: datetime = START_DATE, end: datetime = END_DATE) -> Optional[pd.DataFrame]:
    """Fetch OHLCV data for a single ticker."""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start, end=end)

        if hist.empty:
            logger.warning(f"{ticker}: No data returned")
            return None

        hist = hist.reset_index()
        hist["ticker"] = ticker
        hist["Date"] = pd.to_datetime(hist["Date"]).dt.tz_localize(None)

        logger.info(f"✅ {ticker} ({TICKERS[ticker]['name']}): {len(hist)} rows")
        return hist

    except Exception as e:
        logger.error(f"❌ {ticker}: {e}")
        return None


def fetch_fundamentals(ticker: str) -> dict:
    """Fetch fundamental data for a single ticker."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        fundamentals = {
            "ticker": ticker,
            "company_name": info.get("shortName", TICKERS.get(ticker, {}).get("name", ticker)),
            "sector": info.get("sector", TICKERS.get(ticker, {}).get("sector", "Unknown")),
            "per": info.get("trailingPE", None),
            "pbr": info.get("priceToBook", None),
            "roe": info.get("returnOnEquity", None),
            "market_cap": info.get("marketCap", None),
            "dividend_yield": info.get("dividendYield", None),
            "forward_pe": info.get("forwardPE", None),
            "enterprise_value": info.get("enterpriseValue", None),
            "profit_margins": info.get("profitMargins", None),
            "revenue_growth": info.get("revenueGrowth", None),
        }

        logger.info(f"✅ {ticker} fundamentals: PER={fundamentals['per']}, PBR={fundamentals['pbr']}, ROE={fundamentals['roe']}")
        return fundamentals

    except Exception as e:
        logger.error(f"❌ {ticker} fundamentals: {e}")
        return {"ticker": ticker, "error": str(e)}


def fetch_all_stocks() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch OHLCV and fundamental data for all target stocks.
    Returns (ohlcv_df, fundamentals_df).
    """
    logger.info("=" * 60)
    logger.info("  📊 Japan Stock Trading Signal — Data Fetch")
    logger.info("=" * 60)

    # OHLCV
    logger.info("\n[1/2] Fetching OHLCV data...")
    ohlcv_frames = []
    for ticker in TICKER_LIST:
        df = fetch_ohlcv(ticker)
        if df is not None:
            ohlcv_frames.append(df)

    if not ohlcv_frames:
        raise ValueError("No OHLCV data fetched for any ticker")

    ohlcv_df = pd.concat(ohlcv_frames, ignore_index=True)
    logger.info(f"\n📊 OHLCV Total: {len(ohlcv_df):,} rows, {ohlcv_df['ticker'].nunique()} tickers")

    # Fundamentals
    logger.info("\n[2/2] Fetching fundamental data...")
    fund_list = []
    for ticker in TICKER_LIST:
        fund = fetch_fundamentals(ticker)
        fund_list.append(fund)

    fundamentals_df = pd.DataFrame(fund_list)
    logger.info(f"📊 Fundamentals: {len(fundamentals_df)} tickers")

    return ohlcv_df, fundamentals_df


def save_raw_data(ohlcv_df: pd.DataFrame, fundamentals_df: pd.DataFrame, output_dir: str = DATA_DIR):
    """Save raw data to CSV and JSON files."""
    os.makedirs(output_dir, exist_ok=True)

    # OHLCV
    ohlcv_path = os.path.join(output_dir, "raw_ohlcv.csv")
    ohlcv_df.to_csv(ohlcv_path, index=False)
    logger.info(f"📁 OHLCV CSV: {ohlcv_path}")

    # Fundamentals
    fund_path = os.path.join(output_dir, "raw_fundamentals.csv")
    fundamentals_df.to_csv(fund_path, index=False)
    logger.info(f"📁 Fundamentals CSV: {fund_path}")

    # JSON for each ticker (for BlazingMQ messages)
    messages_dir = os.path.join(output_dir, "messages")
    os.makedirs(messages_dir, exist_ok=True)

    for ticker in ohlcv_df["ticker"].unique():
        ticker_data = ohlcv_df[ohlcv_df["ticker"] == ticker].copy()
        fund_row = fundamentals_df[fundamentals_df["ticker"] == ticker]

        message = {
            "ticker": ticker,
            "company_name": TICKERS.get(ticker, {}).get("name", ticker),
            "sector": TICKERS.get(ticker, {}).get("sector", "Unknown"),
            "ohlcv": json.loads(ticker_data.to_json(orient="records", date_format="iso")),
            "fundamentals": json.loads(fund_row.to_json(orient="records")) if not fund_row.empty else {},
            "metadata": {
                "fetched_at": datetime.now().isoformat(),
                "rows": len(ticker_data),
                "start_date": str(ticker_data["Date"].min()),
                "end_date": str(ticker_data["Date"].max()),
            }
        }

        safe_name = ticker.replace(".", "_")
        msg_path = os.path.join(messages_dir, f"{safe_name}.json")
        with open(msg_path, "w") as f:
            json.dump(message, f, indent=2, default=str)

    logger.info(f"📁 Message JSONs: {ohlcv_df['ticker'].nunique()} files → {messages_dir}/")


if __name__ == "__main__":
    ohlcv_df, fundamentals_df = fetch_all_stocks()
    save_raw_data(ohlcv_df, fundamentals_df)

    # Summary
    print("\n" + "=" * 60)
    print("  📊 Fetch Complete — Summary")
    print("=" * 60)
    for ticker in TICKER_LIST:
        t_data = ohlcv_df[ohlcv_df["ticker"] == ticker]
        if not t_data.empty:
            latest = t_data.iloc[-1]
            print(f"  {ticker:8s} | {TICKERS[ticker]['name']:22s} | ¥{latest['Close']:>10,.0f} | {len(t_data):>4d} rows")
    print("=" * 60)
