import requests
import pandas as pd
import time
import os

symbol = "BTCUSDT"
interval = "1"  # 1-minute
limit = 1000
max_total = 500_000
save_every = 10_000  # Сохраняем каждые N свечей
fname = "BTCUSDT_bybit_500k.csv"

api_url = "https://api.bybit.com/v5/market/kline"

# --- Загружаем текущий файл если есть ---
if os.path.exists(fname):
    df = pd.read_csv(fname)
    if len(df) > 0:
        last_ts = int(df['timestamp'].max())
        print(f"The file is found. Last candle: {pd.to_datetime(last_ts, unit='ms')}")
        start_time = last_ts + 60_000
    else:
        df = pd.DataFrame()
        start_time = None
else:
    df = pd.DataFrame()
    start_time = None

candles = []
total_new = 0

while True:
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    if start_time:
        params["start"] = start_time

    resp = requests.get(api_url, params=params)
    data = resp.json()
    if data["retCode"] != 0:
        print(f"Error: {data['retMsg']}")
        break

    klines = data["result"]["list"]
    if not klines:
        print("No more new data.")
        break

    # Сортируем по времени и фильтруем только новые свечи
    klines = sorted(klines, key=lambda x: int(x[0]))
    klines = [k for k in klines if int(k[0]) >= (start_time or 0)]
    if not klines:
        break

    candles.extend(klines)
    total_new += len(klines)
    print(f"Downloaded new: {total_new} ({len(candles)} in current buffer)")

    # Сохраняем каждые save_every свечей или если дошли до конца данных
    if len(candles) >= save_every or len(klines) < limit:
        df_new = pd.DataFrame(candles, columns=[
            "timestamp", "open", "high", "low", "close", "volume", "turnover"
        ])
        df_new = df_new.astype({
            "timestamp": "int64",
            "open": "float",
            "high": "float",
            "low": "float",
            "close": "float",
            "volume": "float",
            "turnover": "float"
        })
        df = pd.concat([df, df_new], ignore_index=True)
        df = df.drop_duplicates("timestamp")
        df = df.sort_values("timestamp").iloc[-max_total:]
        df.to_csv(fname, index=False)
        print(f"--- File saved. Total rows: {len(df)} ---")
        candles = []  # очищаем буфер

    # Готовим start_time для следующего запроса (следующая минута)
    start_time = int(klines[-1][0]) + 60_000

    # Sleep для антиспама
    time.sleep(1.1) 

print("Download completed.")