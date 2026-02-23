import { useEffect, useState } from 'react';
import axios from 'axios';
import './index.css';

function App() {
  const [stocks, setStocks] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Fetch data from Java Backend
    axios.get('http://localhost:8080/api/stocks')
      .then((res) => {
        setStocks(res.data);
        setLoading(false);
      })
      .catch((err) => {
        console.error("Failed to load stocks:", err);
        setLoading(false);
      });
  }, []);

  if (loading) {
    return <div className="dashboard"><h1>Loading AI Backtests...</h1></div>;
  }

  return (
    <div className="dashboard">
      <h1>JP Backtest</h1>
      <p className="subtitle">AI-powered tracking for Japanese blue-chip stocks.</p>

      <div className="grid">
        {stocks.map((s) => (
          <div className="card" key={s.id}>
            <h3>{s.ticker}</h3>
            <div className="price">
              ¥{s.closePrice.toLocaleString()}
              <span className={`decision-badge decision-${s.decisionType}`}>
                {s.decisionType}
              </span>
            </div>
            <div>{s.companyName}</div>

            <div className="metrics">
              <div className="metric">
                <div className="metric-label">RSI(14)</div>
                <div className="metric-value">{s.rsi14.toFixed(1)}</div>
              </div>
              <div className="metric">
                <div className="metric-label">Fund. Score</div>
                <div className="metric-value">{s.fundamentalScore.toFixed(1)} / 100</div>
              </div>
              <div className="metric">
                <div className="metric-label">AI Confidence</div>
                <div className="metric-value">{(s.confidence * 100).toFixed(0)}%</div>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default App;
