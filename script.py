import requests
import os
import time
from dotenv import load_dotenv
from datetime import  datetime
import snowflake.connector

load_dotenv()

def run_stock_job():
    API_KEY = os.getenv("POLYGON_API_KEY")
    LIMIT = os.getenv("API_PAGINATION_LIMIT")
    DS = os.getenv("DATE_STRING")


    tickers = []
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&sort=ticker&order=asc&limit={LIMIT}&apiKey={API_KEY}"
    response = requests.get(url)
    data = response.json()

    for ticker in data['results']:
        ticker['ds'] = DS
        tickers.append(ticker)

    while data.get('next_url'):
        DS = datetime.now()
        print(f"Fetching next page:", data['next_url'])
        next_url = data['next_url'] + f"&apiKey={API_KEY}"
        response = requests.get(next_url)
        if response.status_code != 200:
            print("Error fetching data:", response.status_code, response.text)
            time.sleep(60)
            response = requests.get(next_url)
        data = response.json()
        for ticker in data['results']:
            ticker['ds'] = DS
            tickers.append(ticker)

    print(f"Total tickers fetched: {len(tickers)}")

    # Snowflake connection parameters
    conn = snowflake.connector.connect(
        user='rk72591',
        password='Krkiit87654321',
        account='aoremyz-vw18807',  # Corrected account identifier
        warehouse='COMPUTE_WH',
        database='ZACHWILSON',
        schema='PUBLIC'
    )
    cursor = conn.cursor()


    insert_sql = """
    INSERT INTO STOCK_TICKERS (
        ticker, name, market, locale, primary_exchange, type, active,
        currency_name, cik, composite_figi, share_class_figi, last_updated_utc, ds
    ) VALUES (
        %(ticker)s, %(name)s, %(market)s, %(locale)s, %(primary_exchange)s, %(type)s, %(active)s,
        %(currency_name)s, %(cik)s, %(composite_figi)s, %(share_class_figi)s, %(last_updated_utc)s, %(ds)s
    )
    """

    # Prepare all rows for batch insert
    rows = [
        {col: ticker.get(col, None) for col in [
            "ticker", "name", "market", "locale", "primary_exchange", "type", "active",
            "currency_name", "cik", "composite_figi", "share_class_figi", "last_updated_utc", "ds"
        ]}
        for ticker in tickers
    ]

    # Batch insert using executemany
    cursor.executemany(insert_sql, rows)

    cursor.close()
    conn.close()
    print("Data inserted into Snowflake table STOCK_TICKERS.")

if __name__ == "__main__":
    run_stock_job()
