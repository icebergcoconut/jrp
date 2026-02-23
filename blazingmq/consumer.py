"""
BlazingMQ Consumer
Receives stock data messages from the queue, validates, and writes
to Delta Lake-ready JSON/Parquet format.

The consumer validates:
- Required fields present (ticker, ohlcv data)
- Data types are correct
- Values are within reasonable ranges
- No duplicate dates per ticker
"""

import json
import os
import logging
import pandas as pd
from datetime import datetime
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

try:
    import blazingmq
    BMQ_AVAILABLE = True
except ImportError:
    BMQ_AVAILABLE = False

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_pipeline.config import BMQ_BROKER_URI, BMQ_QUEUE_URI, DATA_DIR


class MessageValidator:
    """Validates incoming stock data messages."""

    REQUIRED_OHLCV_FIELDS = {"Open", "High", "Low", "Close", "Volume", "Date"}

    @staticmethod
    def validate(message: dict) -> tuple[bool, list[str]]:
        """
        Validate a stock data message.
        Returns (is_valid, list_of_errors).
        """
        errors = []

        # Check ticker
        if "ticker" not in message:
            errors.append("Missing required field: ticker")
            return False, errors

        ticker = message["ticker"]

        # Check OHLCV data
        if "ohlcv" not in message or not message["ohlcv"]:
            errors.append(f"{ticker}: Missing or empty OHLCV data")
            return False, errors

        ohlcv = message["ohlcv"]
        if not isinstance(ohlcv, list):
            errors.append(f"{ticker}: OHLCV data must be a list")
            return False, errors

        # Check first record for required fields
        first_record = ohlcv[0]
        missing_fields = MessageValidator.REQUIRED_OHLCV_FIELDS - set(first_record.keys())
        if missing_fields:
            errors.append(f"{ticker}: Missing OHLCV fields: {missing_fields}")

        # Validate data ranges
        for i, record in enumerate(ohlcv):
            try:
                close = float(record.get("Close", 0))
                if close <= 0:
                    errors.append(f"{ticker} row {i}: Invalid Close price: {close}")

                volume = int(record.get("Volume", 0))
                if volume < 0:
                    errors.append(f"{ticker} row {i}: Negative volume: {volume}")

                high = float(record.get("High", 0))
                low = float(record.get("Low", 0))
                if high < low:
                    errors.append(f"{ticker} row {i}: High ({high}) < Low ({low})")

            except (ValueError, TypeError) as e:
                errors.append(f"{ticker} row {i}: Type error: {e}")

        is_valid = len(errors) == 0
        return is_valid, errors


class StockDataConsumer:
    """
    Consumes stock data messages from BlazingMQ and writes validated
    data to local storage in Delta Lake-ready format.
    """

    def __init__(self, broker_uri: str = BMQ_BROKER_URI, queue_uri: str = BMQ_QUEUE_URI):
        self.broker_uri = broker_uri
        self.queue_uri = queue_uri
        self.session = None
        self.validator = MessageValidator()
        self.messages_received = 0
        self.messages_valid = 0
        self.messages_invalid = 0

    def connect(self):
        """Connect to BlazingMQ broker as consumer."""
        if BMQ_AVAILABLE:
            try:
                self.session = blazingmq.Session(
                    blazingmq.session_options.SessionOptions(broker_uri=self.broker_uri),
                    on_message=self._on_message,
                )
                self.session.open(
                    self.queue_uri,
                    options=blazingmq.QueueOptions(),
                    read=True,
                )
                logger.info(f"✅ Connected to BlazingMQ as consumer")
            except Exception as e:
                logger.error(f"❌ Failed to connect: {e}")
                raise
        else:
            logger.info("BlazingMQ not available — using local file consumption mode")

    def _on_message(self, message, message_handle):
        """Callback for received BlazingMQ messages."""
        try:
            data = json.loads(message.data.decode("utf-8"))
            self.process_message(data)
            message_handle.confirm()
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def process_message(self, message: dict) -> Optional[pd.DataFrame]:
        """Validate and process a single message."""
        self.messages_received += 1
        ticker = message.get("ticker", "unknown")

        # Validate
        is_valid, errors = self.validator.validate(message)

        if not is_valid:
            self.messages_invalid += 1
            for err in errors:
                logger.warning(f"⚠️  Validation error: {err}")
            return None

        self.messages_valid += 1

        # Convert OHLCV to DataFrame
        ohlcv_df = pd.DataFrame(message["ohlcv"])
        ohlcv_df["ticker"] = ticker
        ohlcv_df["Date"] = pd.to_datetime(ohlcv_df["Date"])

        # Remove duplicates
        ohlcv_df = ohlcv_df.drop_duplicates(subset=["ticker", "Date"])

        # Merge fundamentals
        fundamentals = message.get("fundamentals", {})
        if isinstance(fundamentals, list) and fundamentals:
            fundamentals = fundamentals[0]
        elif isinstance(fundamentals, list):
            fundamentals = {}

        for key, value in fundamentals.items():
            if key != "ticker":
                ohlcv_df[key] = value

        ohlcv_df["company_name"] = message.get("company_name", ticker)
        ohlcv_df["sector"] = message.get("sector", "Unknown")

        logger.info(f"✅ Processed: {ticker} — {len(ohlcv_df)} rows (valid)")
        return ohlcv_df

    def consume_from_files(self, data_dir: str = DATA_DIR) -> pd.DataFrame:
        """
        Consume messages from local JSON files (for dev/testing without broker).
        Returns combined DataFrame of all validated data.
        """
        messages_dir = os.path.join(data_dir, "messages")
        if not os.path.exists(messages_dir):
            logger.error(f"Messages directory not found: {messages_dir}")
            return pd.DataFrame()

        all_frames = []
        json_files = sorted([f for f in os.listdir(messages_dir) if f.endswith(".json")])

        logger.info(f"📥 Consuming {len(json_files)} messages from {messages_dir}")

        for filename in json_files:
            filepath = os.path.join(messages_dir, filename)
            with open(filepath, "r") as f:
                message = json.load(f)

            df = self.process_message(message)
            if df is not None:
                all_frames.append(df)

        if all_frames:
            return pd.concat(all_frames, ignore_index=True)
        return pd.DataFrame()

    def write_delta_ready(self, df: pd.DataFrame, output_dir: str = DATA_DIR):
        """Write validated data in Delta Lake-ready format (Parquet + JSON)."""
        if df.empty:
            logger.warning("No data to write")
            return

        output_path = os.path.join(output_dir, "validated")
        os.makedirs(output_path, exist_ok=True)

        # Parquet (Delta Lake-ready)
        parquet_path = os.path.join(output_path, "stock_data.parquet")
        df.to_parquet(parquet_path, index=False)
        logger.info(f"📁 Parquet: {parquet_path}")

        # CSV (for debugging)
        csv_path = os.path.join(output_path, "stock_data.csv")
        df.to_csv(csv_path, index=False)
        logger.info(f"📁 CSV: {csv_path}")

        # JSON (for API)
        json_path = os.path.join(output_path, "stock_data.json")
        df.to_json(json_path, orient="records", date_format="iso", indent=2)
        logger.info(f"📁 JSON: {json_path}")

    def get_stats(self) -> dict:
        return {
            "received": self.messages_received,
            "valid": self.messages_valid,
            "invalid": self.messages_invalid,
            "validation_rate": f"{(self.messages_valid / max(self.messages_received, 1)) * 100:.1f}%",
        }


def main():
    logger.info("=" * 60)
    logger.info("  📥 BlazingMQ Consumer — Stock Data Receiver")
    logger.info("=" * 60)

    consumer = StockDataConsumer()

    # Local file consumption mode
    df = consumer.consume_from_files()

    if not df.empty:
        consumer.write_delta_ready(df)

    stats = consumer.get_stats()
    logger.info("\n" + "=" * 60)
    logger.info(f"  📊 Consumption Complete")
    logger.info(f"  Received: {stats['received']}")
    logger.info(f"  Valid: {stats['valid']}")
    logger.info(f"  Invalid: {stats['invalid']}")
    logger.info(f"  Validation rate: {stats['validation_rate']}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
