# 📈 JP Signal — Japan Stock Trading Signal App

> AI-powered trading signal application for Japanese blue-chip stocks.

## 🏗️ Architecture

```
Yahoo Finance (yfinance)
  → Python Producer
  → BlazingMQ (message queue)
  → Python Consumer
  → Databricks (Delta Lake + feature engineering)
  → AWS SageMaker (XGBoost signal model)
  → Java Spring Boot Backend
  → Cloud App Service (React frontend)
```

## 🎯 Target Stocks

| Ticker | Company | Sector |
|--------|---------|--------|
| 7203.T | Toyota Motor | Automotive |
| 6758.T | Sony Group | Electronics |
| 9984.T | SoftBank Group | Telecom/Tech |
| 6861.T | Keyence | Electronics |
| 6098.T | Recruit Holdings | Services |
| 8306.T | Mitsubishi UFJ FG | Banking |
| 9432.T | NTT | Telecom |
| 6501.T | Hitachi | Electronics |
| 4063.T | Shin-Etsu Chemical | Chemicals |
| 7974.T | Nintendo | Gaming |

## 📊 Features

- **Data Pipeline**: Python-based ingestion from Yahoo Finance, decoupled via BlazingMQ.
- **Data Engineering**: Databricks ecosystem for feature engineering (SMA, RSI, Bollinger Bands).
- **ML-Powered Signals**: XGBoost binary classifier deployed to AWS SageMaker for predicting signals.
- **Enterprise Backend**: Built with Java 21 and Spring Boot 3.x for robust API services.
- **Interactive UI**: React 18 + Vite frontend for premium charting and dashboards.
- **Cloud Native**: Deployable to Cloud App Service with Docker and CI/CD.

## 🏢 Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Data Source | yfinance | OHLCV + fundamentals |
| Message Queue | BlazingMQ (Docker) | Decoupled data ingestion |
| Data Platform | Databricks | Delta Lake, feature engineering |
| ML Model | AWS SageMaker | XGBoost real-time endpoint |
| Backend | Java Spring Boot | REST API |
| Frontend | React + Vite | Interactive charts & Dashboards |
| Hosting | Cloud App Service | Docker container deployment |
| CI/CD | GitHub Actions | Automated delivery |

## 📂 Project Structure

```
research-platform-mini/
├── data_pipeline/     ← yfinance data ingestion
├── blazingmq/         ← Message queue producer/consumer
├── databricks/        ← Databricks notebooks
├── sagemaker/         ← ML training & deployment
├── backtesting/       ← Event-driven backtester (Python)
├── backend/           ← Java Spring Boot Backend
├── frontend/          ← React 18 frontend
├── docker/            ← Docker compositions
├── README.md
└── IMPLEMENTATION_PLAN.md
```

## 🎤 Interview Talking Points

- **Multi-cloud architecture**: Cloud Provider (hosting) + AWS (ML serving) + Databricks (data platform).
- **Message queue expertise**: Bloomberg's BlazingMQ for decoupled, fault-tolerant data ingestion.
- **Enterprise Middleware**: Java Spring Boot backend connecting Data Lakes to Frontend UIs securely.
- **End-to-end ownership**: From raw data ingestion to ML model serving to interactive React UI.
- **Production patterns**: Validation pipeline, health checks, Docker deployment, CI/CD.

## 📄 License

MIT — Built for portfolio demonstration purposes.
