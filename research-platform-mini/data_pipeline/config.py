"""
Configuration for the Japan Stock Trading Signal App.
Defines target stocks, date ranges, and indicator parameters.
"""

from datetime import datetime, timedelta

# ── Target Stocks: 10 Japanese Blue-Chips ──
TICKERS = {
    "7203.T": {"name": "Toyota Motor", "sector": "Automotive"},
    "6758.T": {"name": "Sony Group", "sector": "Electronics"},
    "9984.T": {"name": "SoftBank Group", "sector": "Telecom/Tech"},
    "6861.T": {"name": "Keyence", "sector": "Electronics"},
    "6098.T": {"name": "Recruit Holdings", "sector": "Services"},
    "8306.T": {"name": "Mitsubishi UFJ FG", "sector": "Banking"},
    "9432.T": {"name": "NTT", "sector": "Telecom"},
    "6501.T": {"name": "Hitachi", "sector": "Electronics"},
    "4063.T": {"name": "Shin-Etsu Chemical", "sector": "Chemicals"},
    "7974.T": {"name": "Nintendo", "sector": "Gaming"},
}

TICKER_LIST = list(TICKERS.keys())

# ── Date Ranges ──
HISTORY_DAYS = 730  # 2 years
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=HISTORY_DAYS)

# ── Technical Indicator Parameters ──
SMA_PERIODS = [5, 25, 75]
RSI_PERIOD = 14
BB_PERIOD = 20
BB_STD = 2

# ── Signal Parameters ──
SIGNAL_FORWARD_DAYS = 20      # Look-ahead window for label generation
SIGNAL_THRESHOLD = 0.03       # 3% return threshold for BUY signal

# ── Fundamental Metrics ──
FUNDAMENTAL_METRICS = ["trailingPE", "priceToBook", "returnOnEquity", "marketCap", "dividendYield"]

# ── BlazingMQ ──
BMQ_BROKER_URI = "tcp://localhost:30114"
BMQ_QUEUE_URI = "bmq://bmq.test.mmap.priority/stock-data-queue"

# ── AWS SageMaker ──
SAGEMAKER_ENDPOINT_NAME = "jp-stock-signal-xgboost"
SAGEMAKER_REGION = "ap-northeast-1"

# ── Paths ──
DATA_DIR = "data"
FEATURES_DIR = "data/features"
MODEL_DIR = "data/model"
