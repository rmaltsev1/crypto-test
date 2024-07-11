import ccxt
import pandas as pd
import time

# Initialize the exchange
exchange = ccxt.binance({
    'rateLimit': 1200,  # Exchange's rate limit
    'enableRateLimit': True,
})

symbol = 'BTC/USDT'
timeframe = '15m' 
start_date = '2020-01-01T00:00:00Z'
batch_size = 1500  # Number of candles per batch

# Convert start date to milliseconds
start_timestamp = exchange.parse8601(start_date)

all_candles = []

while True:
    # Fetch ohlcv data
    candles = exchange.fetch_ohlcv(symbol, timeframe, since=start_timestamp, limit=batch_size)
    
    if not candles:
        break
    
    all_candles.extend(candles)
    start_timestamp = candles[-1][0] + 1  # Move to the next batch
    
    # Sleep to avoid hitting the rate limit
    time.sleep(exchange.rateLimit / 1000)

# Convert to DataFrame
df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

# Save to CSV
df.to_csv('BTC_USDT_15m.csv', index=False)

print('Data saved to BTC_USDT_15m.csv')
