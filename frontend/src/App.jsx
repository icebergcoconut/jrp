import { useEffect, useState } from 'react';
import axios from 'axios';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
} from 'chart.js';
import './index.css';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

function App() {
  const [activeTab, setActiveTab] = useState('dashboard');

  // Dashboard state
  const [stocks, setStocks] = useState([]);
  const [loading, setLoading] = useState(true);

  // Backtest state
  const [ticker, setTicker] = useState('7203.T');
  const [period, setPeriod] = useState('2y');
  const [strategy, setStrategy] = useState('RSI');
  const [paramValue, setParamValue] = useState(50);
  const [backtestResult, setBacktestResult] = useState(null);
  const [runningBacktest, setRunningBacktest] = useState(false);

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

  const runBacktest = async (e) => {
    e.preventDefault();
    setRunningBacktest(true);
    try {
      let params = {};
      if (strategy === 'RSI') params = { threshold: paramValue };
      if (strategy === 'SMA_Cross') params = { fast: 20, slow: paramValue };

      const res = await axios.post('http://localhost:8000/api/run_backtest', {
        ticker,
        period,
        strategy,
        params
      });
      if (res.data.error) {
        alert(res.data.error);
      } else {
        setBacktestResult(res.data);
      }
    } catch (err) {
      console.error("Backtest failed", err);
      alert("Backtest failed. Ensure Python API is running on port 8000.");
    }
    setRunningBacktest(false);
  };

  const chartOptions = {
    responsive: true,
    plugins: {
      legend: { position: 'top' },
      title: { display: true, text: 'Cumulative Performance' },
    },
    scales: {
      y: { type: 'linear', display: true, position: 'left' }
    }
  };

  return (
    <div className="dashboard-container">
      <header className="navbar">
        <div className="logo-section">
          <span className="logo">📈 JP Backtest</span>
        </div>
      </header>

      <main className="content dashboard">
        <div className="section-block">
          <h2 className="subtitle">AI-powered tracking for Japanese blue-chip stocks.</h2>
          {loading ? (
            <h1>Loading AI Backtests...</h1>
          ) : (
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
                    <div className="metric tooltip-container">
                      <div className="metric-label">Fund. Score ⓘ</div>
                      <div className="metric-value">{s.fundamentalScore.toFixed(1)} / 100</div>
                      <div className="tooltip">A combined financial health rating based on P/E ratios and recent earnings. Higher means the company balance sheet is stronger.</div>
                    </div>
                    <div className="metric tooltip-container">
                      <div className="metric-label">AI Confidence ⓘ</div>
                      <div className="metric-value">{(s.confidence * 100).toFixed(0)}%</div>
                      <div className="tooltip">The SageMaker XGBoost AI model's percentage confidence level in its current Buy/Sell/Hold prediction based on historical trends.</div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="backtest-panel card" style={{ marginTop: '3rem' }}>
          <h2>Strategy Backtesting Engine</h2>
          <div className="backtest-layout">
            <form className="backtest-form" onSubmit={runBacktest}>
              <div className="form-group">
                <label>Select Stock</label>
                <select value={ticker} onChange={(e) => setTicker(e.target.value)}>
                  <option value="7203.T">Toyota Motor (7203.T)</option>
                  <option value="6758.T">Sony Group (6758.T)</option>
                  <option value="9984.T">SoftBank Group (9984.T)</option>
                  <option value="6861.T">Keyence (6861.T)</option>
                  <option value="6098.T">Recruit Holdings (6098.T)</option>
                  <option value="8306.T">Mitsubishi UFJ FG (8306.T)</option>
                  <option value="9432.T">NTT (9432.T)</option>
                  <option value="6501.T">Hitachi (6501.T)</option>
                  <option value="4063.T">Shin-Etsu Chemical (4063.T)</option>
                  <option value="7974.T">Nintendo (7974.T)</option>
                </select>
              </div>

              <div className="form-group">
                <label>Analysis Period</label>
                <select value={period} onChange={(e) => setPeriod(e.target.value)}>
                  <option value="1y">1 Year</option>
                  <option value="2y">2 Years</option>
                  <option value="5y">5 Years</option>
                  <option value="10y">10 Years</option>
                </select>
              </div>

              <div className="form-group">
                <label>Trading Strategy</label>
                <select value={strategy} onChange={(e) => setStrategy(e.target.value)}>
                  <option value="RSI">RSI Reversion (Buy if RSI &lt; X)</option>
                  <option value="SMA_Cross">Moving Average Crossover</option>
                </select>
              </div>

              <div className="form-group">
                <label>
                  {strategy === 'RSI' ? 'RSI Threshold (ex: 30)' : 'Slow Moving Average (ex: 50 days)'}
                </label>
                <input
                  type="number"
                  value={paramValue}
                  onChange={(e) => setParamValue(Number(e.target.value))}
                  required
                />
              </div>

              <button type="submit" disabled={runningBacktest} className="run-btn">
                {runningBacktest ? 'Running Simulation...' : 'Run Backtest Engine'}
              </button>
            </form>

            <div className="backtest-results">
              {backtestResult ? (
                <>
                  <div className="metrics-grid">
                    {Object.entries(backtestResult.metrics).map(([key, value]) => (
                      <div className="card rm-card" key={key}>
                        <div className="rm-label">{key}</div>
                        <div className={`rm-value ${value < 0 ? 'negative' : 'positive'}`}>
                          {value}
                        </div>
                      </div>
                    ))}
                  </div>
                  <div className="chart-container" style={{ marginTop: '2rem' }}>
                    <Line
                      options={chartOptions}
                      data={{
                        labels: backtestResult.chart.dates,
                        datasets: [
                          {
                            label: 'Stock Setup (Base)',
                            data: backtestResult.chart.base_returns,
                            borderColor: 'rgba(255, 255, 255, 0.4)',
                            borderWidth: 1,
                            pointRadius: 0
                          },
                          {
                            label: 'Strategy Return',
                            data: backtestResult.chart.returns,
                            borderColor: 'rgb(56, 189, 248)',
                            backgroundColor: 'rgba(56, 189, 248, 0.1)',
                            tension: 0.1,
                            borderWidth: 2,
                            pointRadius: 0
                          }
                        ]
                      }}
                    />
                  </div>
                </>
              ) : (
                <div className="empty-state">
                  Please configure and run the engine to execute the strategy simulation against historical data blocks.
                </div>
              )}
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;
