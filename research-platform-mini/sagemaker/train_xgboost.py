"""
SageMaker XGBoost Training Script
Trains a binary classifier to predict BUY (1) vs HOLD (0) signals.

Usage:
  1. Upload training_data.csv to S3
  2. Run this script locally or as a SageMaker training job
  3. Model is saved and can be deployed as a real-time endpoint

Features used:
  - sma_5, sma_25, sma_75 (relative to close)
  - rsi_14
  - bb_upper, bb_lower (relative to close)
  - fundamental_score
  - per, pbr, roe, dividend_yield
"""

import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, classification_report, confusion_matrix
)
import json
import os
import logging
import pickle

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def prepare_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """
    Prepare feature matrix and labels from raw training data.
    All features are normalized relative to the close price where applicable.
    """
    features = pd.DataFrame()

    # Technical indicators (normalized to close price)
    features["sma_5_ratio"] = df["sma_5"] / df["Close"] - 1
    features["sma_25_ratio"] = df["sma_25"] / df["Close"] - 1
    features["sma_75_ratio"] = df["sma_75"] / df["Close"] - 1

    # SMA crossover signals
    features["sma_25_above_75"] = (df["sma_25"] > df["sma_75"]).astype(int)
    features["close_above_sma25"] = (df["Close"] > df["sma_25"]).astype(int)

    # RSI
    features["rsi_14"] = df["rsi_14"] / 100  # Normalize to 0-1

    # Bollinger Bands (distance from bands)
    features["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["Close"]
    features["bb_position"] = (df["Close"] - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"])

    # Fundamental score
    features["fundamental_score"] = df["fundamental_score"] / 100

    # Individual fundamentals
    features["per_norm"] = np.clip(df["per"].fillna(15) / 50, 0, 1)
    features["pbr_norm"] = np.clip(df["pbr"].fillna(1.5) / 10, 0, 1)
    features["roe_norm"] = np.clip(df["roe"].fillna(0.1), 0, 0.5) / 0.5

    # Dividend yield
    features["dividend_yield"] = df["dividend_yield"].fillna(0) * 100

    # Volume momentum
    features["volume_ma_ratio"] = df["Volume"] / df.groupby("ticker")["Volume"].transform(
        lambda x: x.rolling(20, min_periods=1).mean()
    )

    # Price momentum
    features["return_5d"] = df.groupby("ticker")["Close"].transform(
        lambda x: x.pct_change(5)
    )
    features["return_20d"] = df.groupby("ticker")["Close"].transform(
        lambda x: x.pct_change(20)
    )

    # Volatility
    features["volatility_20d"] = df.groupby("ticker")["Close"].transform(
        lambda x: x.pct_change().rolling(20).std()
    )

    # Drop NaN rows
    valid_mask = features.notna().all(axis=1) & df["label"].notna()
    features = features[valid_mask]
    labels = df.loc[valid_mask, "label"].astype(int)

    logger.info(f"Feature matrix: {features.shape}")
    logger.info(f"Label distribution: {labels.value_counts().to_dict()}")

    return features, labels


def train_model(features: pd.DataFrame, labels: pd.Series) -> xgb.XGBClassifier:
    """Train XGBoost binary classifier with time-series-aware split."""

    # Time-series split (don't leak future data)
    split_idx = int(len(features) * 0.8)
    X_train, X_test = features.iloc[:split_idx], features.iloc[split_idx:]
    y_train, y_test = labels.iloc[:split_idx], labels.iloc[split_idx:]

    # Handle class imbalance
    scale_pos_weight = (y_train == 0).sum() / max((y_train == 1).sum(), 1)

    logger.info(f"Train: {len(X_train)} | Test: {len(X_test)}")
    logger.info(f"Scale pos weight: {scale_pos_weight:.2f}")

    # XGBoost model
    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,
        eval_metric="auc",
        random_state=42,
        use_label_encoder=False,
    )

    model.fit(
        X_train, y_train,
        eval_set=[(X_test, y_test)],
        verbose=50,
    )

    # Evaluate
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    logger.info("\n" + "=" * 50)
    logger.info("  Model Evaluation")
    logger.info("=" * 50)
    logger.info(f"Accuracy:  {accuracy_score(y_test, y_pred):.4f}")
    logger.info(f"Precision: {precision_score(y_test, y_pred):.4f}")
    logger.info(f"Recall:    {recall_score(y_test, y_pred):.4f}")
    logger.info(f"F1 Score:  {f1_score(y_test, y_pred):.4f}")
    logger.info(f"AUC-ROC:   {roc_auc_score(y_test, y_prob):.4f}")
    logger.info("\nClassification Report:")
    logger.info(classification_report(y_test, y_pred, target_names=["HOLD", "BUY"]))

    # Feature importance
    importance = dict(zip(features.columns, model.feature_importances_))
    sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)
    logger.info("\nFeature Importance (top 10):")
    for name, imp in sorted_imp[:10]:
        logger.info(f"  {name:25s} {imp:.4f}")

    return model


def save_model(model: xgb.XGBClassifier, output_dir: str = "data/model"):
    """Save model for deployment."""
    os.makedirs(output_dir, exist_ok=True)

    # XGBoost native format
    model_path = os.path.join(output_dir, "xgboost_model.json")
    model.save_model(model_path)
    logger.info(f"✅ Model saved: {model_path}")

    # Pickle for SageMaker
    pickle_path = os.path.join(output_dir, "model.pkl")
    with open(pickle_path, "wb") as f:
        pickle.dump(model, f)
    logger.info(f"✅ Pickle saved: {pickle_path}")

    # Feature names
    meta = {
        "feature_names": list(model.feature_names_in_) if hasattr(model, 'feature_names_in_') else [],
        "n_features": model.n_features_in_ if hasattr(model, 'n_features_in_') else 0,
        "model_type": "XGBClassifier",
        "framework": "xgboost",
    }
    meta_path = os.path.join(output_dir, "model_metadata.json")
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    logger.info(f"✅ Metadata saved: {meta_path}")


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("  🤖 XGBoost Signal Model Training")
    logger.info("=" * 60)

    # Load training data
    data_path = "data/training_data.csv"
    if not os.path.exists(data_path):
        # Try alternative paths
        alt_paths = [
            "data/validated/stock_data.csv",
            "data/raw_ohlcv.csv",
        ]
        for p in alt_paths:
            if os.path.exists(p):
                data_path = p
                break

    if not os.path.exists(data_path):
        logger.error("No training data found. Run data pipeline first.")
        logger.info("Generating demo training data...")

        # Generate synthetic data for demo
        np.random.seed(42)
        n = 5000
        demo = pd.DataFrame({
            "ticker": np.random.choice(["7203.T", "6758.T", "9984.T"], n),
            "Date": pd.date_range("2024-01-01", periods=n, freq="h"),
            "Close": 3000 + np.cumsum(np.random.randn(n) * 10),
            "Open": 3000 + np.cumsum(np.random.randn(n) * 10),
            "High": 3050 + np.cumsum(np.random.randn(n) * 10),
            "Low": 2950 + np.cumsum(np.random.randn(n) * 10),
            "Volume": np.random.randint(1e6, 1e7, n),
            "sma_5": 3000 + np.cumsum(np.random.randn(n) * 8),
            "sma_25": 3000 + np.cumsum(np.random.randn(n) * 5),
            "sma_75": 3000 + np.cumsum(np.random.randn(n) * 3),
            "rsi_14": np.clip(50 + np.random.randn(n) * 15, 0, 100),
            "bb_upper": 3100 + np.cumsum(np.random.randn(n) * 10),
            "bb_lower": 2900 + np.cumsum(np.random.randn(n) * 10),
            "fundamental_score": np.random.uniform(30, 80, n),
            "per": np.random.uniform(8, 35, n),
            "pbr": np.random.uniform(0.5, 5, n),
            "roe": np.random.uniform(0.02, 0.25, n),
            "dividend_yield": np.random.uniform(0, 0.04, n),
            "label": np.random.choice([0, 1], n, p=[0.75, 0.25]),
        })
        df = demo
    else:
        df = pd.read_csv(data_path)

    logger.info(f"Loaded: {len(df):,} rows")

    # Prepare features
    features, labels = prepare_features(df)

    # Train model
    model = train_model(features, labels)

    # Save
    save_model(model)

    logger.info("\n✅ Training complete!")
