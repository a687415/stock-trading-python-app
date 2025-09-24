import requests
import os
import csv
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
import time


def run_stock_job():
    API_KEY = os.getenv("POLYGON_API_KEY")
    LIMIT = os.getenv("API_PAGINATION_LIMIT")
    tickers = []
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&sort=ticker&order=asc&limit={LIMIT}&apiKey={API_KEY}"
    response = requests.get(url)
    data = response.json()

    for ticker in data['results']:
        tickers.append(ticker)

    while data.get('next_url'):
        print(f"Fetching next page:", data['next_url'])
        next_url = data['next_url'] + f"&apiKey={API_KEY}"
        response = requests.get(next_url)
        if response.status_code != 200:
            print("Error fetching data:", response.status_code, response.text)
            time.sleep(60)
            response = requests.get(next_url)
        data = response.json()
        for ticker in data['results']:
            tickers.append(ticker)
        

    print(len(tickers))

    # Write to CSV with schema defined for ticker_symbol
    csv_columns = [
        "ticker", "name", "market", "locale", "primary_exchange", "type", "active",
        "currency_name", "cik", "composite_figi", "share_class_figi", "last_updated_utc"
    ]
    with open("tickers.csv", "w", newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for ticker in tickers:
            row = {col: ticker.get(col, "") for col in csv_columns}
            writer.writerow(row)

if __name__ == "__main__":
    run_stock_job()

