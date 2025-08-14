import os
import requests
import psycopg2
from datetime import datetime

def fetch_and_store_stock_data():
    api_key = os.getenv("API_KEY")
    symbol = os.getenv("STOCK_SYMBOL")
    db_user = os.getenv("POSTGRES_USER", "airflow")
    db_pass = os.getenv("POSTGRES_PASSWORD", "airflow")
    db_name = os.getenv("POSTGRES_DB", "airflow")
    db_host = "postgres"

    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if "Time Series (Daily)" not in data:
            raise ValueError("API response missing expected data.")

        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_pass
        )
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                date DATE PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT
            )
        """)

        for date_str, metrics in data["Time Series (Daily)"].items():
            cur.execute("""
                INSERT INTO stock_data (date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (date) DO UPDATE
                SET open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """, (
                datetime.strptime(date_str, "%Y-%m-%d"),
                float(metrics["1. open"]),
                float(metrics["2. high"]),
                float(metrics["3. low"]),
                float(metrics["4. close"]),
                int(metrics["5. volume"])
            ))

        conn.commit()
        cur.close()
        conn.close()
        print("Stock data updated successfully.")

    except Exception as e:
        print(f"Error: {e}")
