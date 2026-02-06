import yfinance as yf
import pandas as pd
import sqlite3
import numpy as np

# --- Task 1: Python Data Generation (Simulation) ---

# 1. ดึงข้อมูลย้อนหลัง 1 ปี (LSE Stocks)
tickers = ['RR.L', 'AML.L', 'TSCO.L']
data = yf.download(tickers, period='1y', interval='1d')['Close']

trade_records = []
trade_id = 1

for ticker in tickers:
    df = data[[ticker]].dropna().copy()
    df.columns = ['Close']
    
    # คำนวณ Simple Moving Average (SMA 20)
    df['SMA20'] = df['Close'].rolling(window=20).mean()
    
    # Mock Trading Logic: 
    # Buy เมื่อราคา > SMA20 และครั้งก่อนหน้า < SMA20 (Golden Cross แบบง่าย)
    # Sell เมื่อราคา < SMA20 และครั้งก่อนหน้า > SMA20
    df['Signal'] = 0
    df.loc[df['Close'] > df['SMA20'], 'Signal'] = 1  # Buy Condition
    df.loc[df['Close'] < df['SMA20'], 'Signal'] = -1 # Sell Condition
    
    # กรองเฉพาะจุดที่มีการเปลี่ยน Signal (Action)
    df['Action_Change'] = df['Signal'].diff()
    trades = df[df['Action_Change'] != 0].copy()
    
    for date, row in trades.iterrows():
        action = 'BUY' if row['Signal'] == 1 else 'SELL'
        if pd.isna(row['SMA20']): continue # ข้ามช่วงที่ยังไม่มีค่าเฉลี่ย
        
        trade_records.append({
            'Trade_ID': trade_id,
            'Date': date.strftime('%Y-%m-%d'),
            'Ticker': ticker,
            'Action': action,
            'Price': round(row['Close'], 2),
            'Quantity': 1000,
            'Commission': 5.00 # Flat fee ในหน่วย GBP
        })
        trade_id += 1

trade_history = pd.DataFrame(trade_records)

# --- Task 2: SQL Implementation ---

# สร้าง/เชื่อมต่อ Database
conn = sqlite3.connect('portfolio.db')
trade_history.to_sql('trades', conn, if_exists='replace', index=False)

def run_query(query, title):
    print(f"\n--- {title} ---")
    print(pd.read_sql_query(query, conn))

# Query A: Performance Tracking (Using LAG Window Function)
# โจทย์: จับคู่รายการ SELL กับ BUY ก่อนหน้าเพื่อคำนวณกำไรต่อรอบ (Trade Profit)
query_a = """
SELECT 
    Trade_ID,
    Ticker,
    Date as Exit_Date,
    Price as Exit_Price,
    Prev_Price as Entry_Price,
    (Price - Prev_Price) * Quantity - (Commission * 2) as Net_Profit_GBP
FROM (
    SELECT *,
           LAG(Price) OVER (PARTITION BY Ticker ORDER BY Date) as Prev_Price,
           LAG(Action) OVER (PARTITION BY Ticker ORDER BY Date) as Prev_Action
    FROM trades
)
WHERE Action = 'SELL' AND Prev_Action = 'BUY';
"""

# Query B: Portfolio Analytics (Using CTEs)
# โจทย์: วิเคราะห์ Win Rate และกำไรรวมแยกตามหุ้น
query_b = """
WITH TradePerf AS (
    SELECT 
        Ticker,
        (Price - LAG(Price) OVER (PARTITION BY Ticker ORDER BY Date)) * Quantity - (Commission * 2) as Profit
    FROM trades
)
SELECT 
    Ticker,
    COUNT(*) as Total_Trades,
    ROUND(SUM(CASE WHEN Profit > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as Win_Rate_Pct,
    ROUND(SUM(Profit), 2) as Total_Net_Profit_GBP
FROM TradePerf
WHERE Profit IS NOT NULL
GROUP BY Ticker
ORDER BY Total_Net_Profit_GBP DESC;
"""
# Query C: Risk Analysis - Maximum Drawdown per Ticker
# นี่คือ Query ปราบเซียนที่ใช้ Window Function ซ้อนกัน (Nested Window Functions)
query_c = """
WITH Cumulative_PL AS (
    SELECT 
        Ticker,
        Date,
        (Price - LAG(Price) OVER (PARTITION BY Ticker ORDER BY Date)) * Quantity - (Commission * 2) as Trade_PL
    FROM trades
),
Running_Balance AS (
    SELECT 
        Ticker,
        Date,
        SUM(Trade_PL) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as Equity_Curve
    FROM Cumulative_PL
    WHERE Trade_PL IS NOT NULL
),
Drawdown_Calc AS (
    SELECT 
        Ticker,
        Date,
        Equity_Curve,
        MAX(Equity_Curve) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as Peak_Equity
    FROM Running_Balance
)
SELECT 
    Ticker,
    MIN(Equity_Curve - Peak_Equity) as Max_Drawdown_GBP
FROM Drawdown_Calc
GROUP BY Ticker;
"""



run_query(query_a, "Query A: Realized Profit/Loss per Trade (Window Function)")
run_query(query_b, "Query B: Ticker Performance Analytics (CTE)")
run_query(query_c, "Query C: Maximum Drawdown (Risk Metric)")

conn.close()