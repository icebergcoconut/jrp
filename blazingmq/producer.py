"""
BlazingMQ Producer
Publishes stock data messages (OHLCV + fundamentals) to BlazingMQ queue.
Each ticker's daily data is sent as a JSON message.

Note: BlazingMQ Python SDK (blazingmq) must be installed.
Run BlazingMQ broker via Docker first (see docker-compose.bmq.yml).
"""

import json
import os
import logging
import time
from datetime import datetime
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Conditional import — falls back to mock for local dev without BlazingMQ
try:
    import blazingmq
    BMQ_AVAILABLE = True
except ImportError:
    BMQ_AVAILABLE = False
    logger.warning("blazingmq not installed. Using mock producer for local development.")

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_pipeline.config import BMQ_BROKER_URI, BMQ_QUEUE_URI, TICKERS, DATA_DIR


class MockSession:
    """Mock BlazingMQ session for local development without broker."""

    def open(self, uri, options):
        logger.info(f"[MOCK] Opened queue: {uri}")

    def post(self, uri, message):
        data = json.loads(message)
        logger.info(f"[MOCK] Posted message for {data.get('ticker', '?')} ({len(message)} bytes)")

    def close(self, uri):
        logger.info(f"[MOCK] Closed queue: {uri}")

    def stop(self):
        logger.info("[MOCK] Session stopped")


class StockDataProducer:
    """
    Publishes stock data to BlazingMQ queue.
    Each message contains a single ticker's complete data payload.
    """

    def __init__(self, broker_uri: str = BMQ_BROKER_URI, queue_uri: str = BMQ_QUEUE_URI):
        self.broker_uri = broker_uri
        self.queue_uri = queue_uri
        self.session = None
        self.messages_sent = 0
        self.errors = 0

    def connect(self):
        """Establish connection to BlazingMQ broker."""
        if BMQ_AVAILABLE:
            try:
                self.session = blazingmq.Session(
                    blazingmq.session_options.SessionOptions(broker_uri=self.broker_uri)
                )
                self.session.open(
                    self.queue_uri,
                    options=blazingmq.QueueOptions(),
                    write=True,
                )
                logger.info(f"✅ Connected to BlazingMQ broker at {self.broker_uri}")
                logger.info(f"✅ Opened queue: {self.queue_uri}")
            except Exception as e:
                logger.error(f"❌ Failed to connect to BlazingMQ: {e}")
                logger.info("Falling back to mock session")
                self.session = MockSession()
                self.session.open(self.queue_uri, None)
        else:
            self.session = MockSession()
            self.session.open(self.queue_uri, None)

    def publish_message(self, message: dict) -> bool:
        """Publish a single JSON message to the queue."""
        try:
            payload = json.dumps(message, default=str)

            if BMQ_AVAILABLE and not isinstance(self.session, MockSession):
                self.session.post(self.queue_uri, payload.encode("utf-8"))
            else:
                self.session.post(self.queue_uri, payload)

            self.messages_sent += 1
            return True

        except Exception as e:
            logger.error(f"❌ Failed to publish message: {e}")
            self.errors += 1
            return False

    def publish_stock_data(self, data_dir: str = DATA_DIR):
        """Read saved JSON messages and publish each to the queue."""
        messages_dir = os.path.join(data_dir, "messages")

        if not os.path.exists(messages_dir):
            logger.error(f"Messages directory not found: {messages_dir}")
            logger.info("Run data_pipeline/fetcher.py first to generate message files.")
            return

        json_files = sorted([f for f in os.listdir(messages_dir) if f.endswith(".json")])
        logger.info(f"Found {len(json_files)} message files to publish")

        for filename in json_files:
            filepath = os.path.join(messages_dir, filename)
            with open(filepath, "r") as f:
                message = json.load(f)

            # Add publish metadata
            message["publish_metadata"] = {
                "published_at": datetime.now().isoformat(),
                "source": "yfinance",
                "schema_version": "1.0",
            }

            ticker = message.get("ticker", "unknown")
            rows = message.get("metadata", {}).get("rows", 0)
            success = self.publish_message(message)

            if success:
                logger.info(f"📤 Published: {ticker} ({rows} rows)")
            else:
                logger.error(f"❌ Failed: {ticker}")

            # Small delay to avoid overwhelming the broker
            time.sleep(0.1)

    def disconnect(self):
        """Close connection to BlazingMQ broker."""
        if self.session:
            try:
                if BMQ_AVAILABLE and not isinstance(self.session, MockSession):
                    self.session.close(self.queue_uri)
                else:
                    self.session.close(self.queue_uri)
                self.session.stop()
                logger.info("✅ Disconnected from BlazingMQ")
            except Exception as e:
                logger.error(f"Error disconnecting: {e}")

    def get_stats(self) -> dict:
        """Return publishing statistics."""
        return {
            "messages_sent": self.messages_sent,
            "errors": self.errors,
            "success_rate": f"{(self.messages_sent / max(self.messages_sent + self.errors, 1)) * 100:.1f}%",
        }


def main():
    logger.info("=" * 60)
    logger.info("  📤 BlazingMQ Producer — Stock Data Publisher")
    logger.info("=" * 60)

    producer = StockDataProducer()

    try:
        producer.connect()
        producer.publish_stock_data()
    finally:
        stats = producer.get_stats()
        logger.info("\n" + "=" * 60)
        logger.info(f"  📊 Publishing Complete")
        logger.info(f"  Messages sent: {stats['messages_sent']}")
        logger.info(f"  Errors: {stats['errors']}")
        logger.info(f"  Success rate: {stats['success_rate']}")
        logger.info("=" * 60)
        producer.disconnect()


if __name__ == "__main__":
    main()
