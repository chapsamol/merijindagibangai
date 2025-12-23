import redis
import json

# --- Configuration ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
# This pattern depends on how you store your stock data. 
# Adjust the pattern to match your Redis key structure (e.g., 'ticker:*' or 'stats:*')
KEY_PATTERN = 'stock_data:*' 
SMA_THRESHOLD = 1000

def check_sma_count():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        
        # 1. Fetch all keys matching the pattern
        keys = r.keys(KEY_PATTERN)
        
        count = 0
        qualified_stocks = []
        
        print(f"Scanning {len(keys)} stocks...")

        for key in keys:
            # 2. Get the data (assuming it's stored as a JSON string)
            data_raw = r.get(key)
            if not data_raw:
                continue
                
            try:
                data = json.loads(data_raw)
                
                # 3. Extract Volume SMA (adjust key name if yours is 'vol_sma' or similar)
                vol_sma = data.get('volume_sma', 0)
                
                if vol_sma > SMA_THRESHOLD:
                    count += 1
                    symbol = key.split(':')[-1] # Extracts 'RBA' from 'stock_data:RBA'
                    qualified_stocks.append(symbol)
                    
            except (json.JSONDecodeError, TypeError):
                continue

        # 4. Output results
        print("-" * 30)
        print(f"TOTAL STOCKS FOUND: {len(keys)}")
        print(f"STOCKS WITH VOL SMA > {SMA_THRESHOLD}: {count}")
        print("-" * 30)
        print(f"Qualified Symbols: {', '.join(qualified_stocks)}")
        
        return count

    except Exception as e:
        print(f"Error connecting to Redis: {e}")

if __name__ == "__main__":
    check_sma_count()