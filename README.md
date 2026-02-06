# UK Stock Trading Journal Database 🇬🇧

## Project Overview
This project simulates an algorithmic trading strategy for key UK equities (**Rolls-Royce, Tesco, Aston Martin**) using Python and analyzes the performance using advanced SQL. The goal is to demonstrate end-to-end data engineering and financial analytics capabilities suitable for asset management workflows.

## Objectives
* **Data Simulation:** Fetch historical data from LSE via `yfinance` and simulate a Moving Average Crossover strategy.
* **Database Design:** Store trade logs in a SQLite database to mimic a transactional ledger.
* **Advanced Analytics:** Use **SQL Window Functions (LAG)** and **CTEs** to calculate P&L per trade, Win Rates, and Maximum Drawdown.

## Key Technologies
* **Python:** pandas, numpy, yfinance
* **SQL:** SQLite, Window Functions, Complex Aggregations

## SQL Implementation Highlights
### 1. Realized P&L Calculation (Window Functions)
Logic: Matches sell orders with their corresponding previous buy orders using `LAG()` to calculate net profit.

### 2. Risk Analysis (Maximum Drawdown)
Logic: Calculates the peak-to-trough decline in portfolio equity to assess downside risk.

## Results
![Query Results](results.png)

* **Top Performer:** Rolls-Royce (RR.L) showed significant upside but with high volatility.
* **Risk Note:** Aston Martin (AML.L) demonstrated the risks of trend-following strategies in a downtrend market.

## How to Run
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
