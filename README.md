# 📈 JP Backtest — Japan Stock Backtesting Platform

**JP Backtest** is an AI-powered backtesting application designed to analyze and evaluate trading strategies on Japanese blue-chip stocks. 

Whether you are a software engineer exploring scalable cloud architectures, or a curious investor wanting to see how artificial intelligence can simulate historical trading decisions, this project serves as a comprehensive demonstration of how modern data pipelines and machine learning algorithms work together.

## 🌟 What does this application do?

In simple terms, **backtesting** is the process of testing a trading strategy using historical data to see how it would have performed before risking any actual money.

This application automates that entire process:
1. It downloads daily stock price data (like Opening and Closing prices) for major Japanese companies (like Toyota, Sony, and Nintendo).
2. It calculates technical patterns (like moving averages).
3. It uses an Artificial Intelligence (AI) model to look at these patterns and decide whether it should have theoretically issued a **BUY**, **SELL**, or **HOLD** decision for that day.
4. It displays these calculated decisions and stock metrics on a beautiful, interactive web dashboard.

## 🏗️ How It Works (The Technology Behind It)

This project bridges together several industry-grade technologies to create a seamless pipeline. Here is the journey of the data from start to finish, explained simply:

### 1. Data Collection (`yfinance` & Python)
We use Python to automatically pull the latest stock market data from Yahoo Finance. Think of this as our data gathering robot.

### 2. Message Queuing (BlazingMQ)
Instead of forcing our application to process all the incoming data immediately, we place the data into a "queue" using **BlazingMQ** (a technology developed by Bloomberg). This acts like a digital post office, safely storing the incoming data chunks and delivering them to the next steps at a manageable, steady pace so the system is never overwhelmed.

### 3. Data Processing (Databricks)
Once the data is delivered, it goes into **Databricks**. You can think of Databricks as an immensely powerful cloud-based spreadsheet calculator. It cleans up the raw stock data and calculates complex financial indicators like RSI (Relative Strength Index) and moving averages.

### 4. Artificial Intelligence (AWS SageMaker)
The cleaned, mathematically enriched data is then sent to **Amazon Web Services (AWS) SageMaker**. This is the "brain" of the operation. We trained an XGBoost Machine Learning model to look at the historical patterns and make a prediction on whether the stock looks favorable. It evaluates the strategies and returns a Backtest Decision.

### 5. Backend Server (Java Spring Boot)
Our **Java Spring Boot** application acts as the reliable middleman (or a restaurant waiter). It reaches out to Databricks and AWS to gather the final calculated numbers and AI predictions, packaging them securely so they can be sent to the user's screen.

### 6. Interactive Dashboard (React + Vite)
Finally, everything is displayed on a modern **React** website. This is the visual interface where users can easily read the data, view charts, and see the AI's backtesting decisions in real-time.

---

## 🎯 Target Stocks

The AI tracks and evaluates backtests for a curated list of top Japanese companies:
* Toyota Motor (7203.T)
* Sony Group (6758.T)
* SoftBank Group (9984.T)
* Keyence (6861.T)
* Recruit Holdings (6098.T)
* Mitsubishi UFJ FG (8306.T)
* NTT (9432.T)
* Hitachi (6501.T)
* Shin-Etsu Chemical (4063.T)
* Nintendo (7974.T)

---

## 🚀 How to Run the Application Locally

If you want to view the source code and run the application on your own computer, please follow these steps. 

> **Important Setup Note (macOS Users)**: To avoid permission issues when running development servers, ensure you clone or run this project in a standard folder like your `Desktop` or `Documents` directory. **Do not run it inside an iCloud Drive syncing folder**, as this restricts necessary permissions for Java and Node.js.

### Step 1: Start the Backend (Java)
The Java backend serves the data via a secure API.
1. Open a terminal and navigate to the `backend` folder:  
   `cd backend`
2. Start the Spring Boot server using Maven:  
   `./mvnw clean spring-boot:run`
3. Leave this terminal running. The API will now be accessible locally.

### Step 2: Start the Frontend (React)
The frontend website fetches the data from the Java backend and paints the dashboard.
1. Open a *new* terminal window.
2. Navigate to the `frontend` folder:  
   `cd frontend`
3. Install the required Node packages (only needed the first time):  
   `npm install`
4. Start the development server:  
   `npm run dev`
5. Open your web browser and navigate to the `localhost` link provided in the terminal to view the interactive application!

### Optional: Manual Data Pipeline Run
The dashboard relies on data already assembled locally in the repository. If you are a developer and wish to simulate the Python data extraction locally:
1. Ensure your Python environment is set up.
2. Run the main test pipeline script from the root of the project:  
   `python3 test_pipeline.py`

---

## 📄 License
MIT — Built for educational and portfolio demonstration purposes.
