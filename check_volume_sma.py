import redis
import json
import os

# --- Configuration ---
# On Heroku, the Redis URL is stored in an environment variable (usually REDIS_URL)
# We use from_url to handle the connection string automatically
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')

# This pattern depends on how you store your stock data. 
# Adjust the pattern to match your Redis key structure
KEY_PATTERN = 'stock_data:*' 
SMA_THRESHOLD = 1000

def check_sma_count():
    """
    Scans Redis for stock keys, parses JSON data, and counts those with Volume SMA > Threshold.
    Uses scan_iter for better performance with large datasets.
    """
    try:
        # Use from_url for production compatibility (SSL, passwords, etc. are handled)
        r = redis.from_url(REDIS_URL, decode_responses=True)
        
        # Verify connection
        r.ping()
        
        count = 0
        total_scanned = 0
        qualified_stocks = []
        
        print(f"Connected to Redis via {REDIS_URL[:20]}...")
        print(f"Filtering stocks with Volume SMA > {SMA_THRESHOLD}...")

        # 1. Use scan_iter instead of keys() to avoid blocking the Redis server
        for key in r.scan_iter(KEY_PATTERN):
            total_scanned += 1
            
            # 2. Get the data
            data_raw = r.get(key)
            if not data_raw:
                continue
                
            try:
                data = json.loads(data_raw)
                
                # 3. Extract Volume SMA
                vol_sma = data.get('volume_sma', 0)
                
                if vol_sma > SMA_THRESHOLD:
                    count += 1
                    symbol = key.split(':')[-1] 
                    qualified_stocks.append(symbol)
                    
            except (json.JSONDecodeError, TypeError):
                continue

        # 4. Output results
        print("-" * 40)
        print(f"SCAN SUMMARY")
        print("-" * 40)
        print(f"Total keys checked: {total_scanned}")
        print(f"Qualified stocks:   {count}")
        print("-" * 40)
        
        if qualified_stocks:
            print(f"Symbols: {', '.join(qualified_stocks)}")
        else:
            print("No stocks met the criteria.")
        
        return count

    except redis.ConnectionError as e:
        print(f"Error: Could not connect to Redis. {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    check_sma_count()