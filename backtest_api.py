from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import yfinance as yf
import pandas as pd
import numpy as np
import quantstats as qs

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class BacktestRequest(BaseModel):
    ticker: str
    period: str
    strategy: str
    params: dict

@app.post("/api/run_backtest")
def run_backtest(req: BacktestRequest):
    try:
        # Fetch data
        df = yf.download(req.ticker, period=req.period)
        if len(df) == 0:
            return {"error": "No data found for this ticker and period"}
            
        close = df['Close']
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        close = close.squeeze()
        
        # Calculate strategy
        if req.strategy == 'RSI':
            # Buy when RSI < threshold, sell when > 70
            threshold = float(req.params.get('threshold', 50))
            delta = close.diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            # Simple simulation: hold when RSI < threshold
            signal = (rsi < threshold).astype(int)
            position = signal.shift(1).fillna(0)
            
        elif req.strategy == 'SMA_Cross':
            fast = int(req.params.get('fast', 20))
            slow = int(req.params.get('slow', 50))
            sma_fast = close.rolling(window=fast).mean()
            sma_slow = close.rolling(window=slow).mean()
            signal = (sma_fast > sma_slow).astype(int)
            position = signal.shift(1).fillna(0)
            
        else:
            position = pd.Series(1, index=close.index) # Buy and hold
            
        # Daily Returns and filtering NaNs
        daily_returns = close.pct_change()
        strategy_returns = daily_returns * position
        strategy_returns = strategy_returns.dropna()
        
        # Make sure index matches quantstats expectation (datetime)
        strategy_returns.index = pd.to_datetime(strategy_returns.index)
        
        # Calculate QuantStats metrics
        sharpe = float(qs.stats.sharpe(strategy_returns))
        cagr = float(qs.stats.cagr(strategy_returns))
        max_dd = float(qs.stats.max_drawdown(strategy_returns))
        win_rate = float(qs.stats.win_rate(strategy_returns))
        volatility = float(qs.stats.volatility(strategy_returns))
        
        # Chart Data
        cum_ret = (1 + strategy_returns).cumprod()
        dates = [d.strftime('%Y-%m-%d') for d in cum_ret.index]
        values = cum_ret.values.tolist()
        
        # Base asset cumulative return
        base_cum_ret = (1 + daily_returns.dropna()).cumprod()
        base_values = base_cum_ret.reindex(cum_ret.index).ffill().values.tolist()
        
        # Round and clean for JSON friendliness
        def clean_val(v):
            if pd.isna(v) or np.isnan(v) or np.isinf(v):
                return 0.0
            return round(v, 2)
            
        return {
            "metrics": {
                "Sharpe Ratio": clean_val(sharpe),
                "CAGR (%)": clean_val(cagr * 100),
                "Max Drawdown (%)": clean_val(max_dd * 100),
                "Win Rate (%)": clean_val(win_rate * 100),
                "Volatility (%)": clean_val(volatility * 100)
            },
            "chart": {
                "dates": dates,
                "returns": values,
                "base_returns": base_values
            }
        }
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
