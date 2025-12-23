import redis
import json
import os

# --- Configuration ---
# Heroku uses REDIS_URL environment variable
REDIS_URL = os.environ.get('REDIS_URL', 'redis://localhost:6379')

# Based on your TradeControl code, keys are likely saved by instrument token.
# We will scan for all keys that look like market data. 
# If your TradeControl uses a prefix like 'market_data:', update this pattern.
KEY_PATTERN = '*' 
SMA_THRESHOLD = 1000

def check_sma_count():
    """
    Scans Redis for stock data synced by the Sync_Command script.
    Filters stocks where calculated SMA > 1000.
    """
    try:
        # Connect with SSL fix for Heroku
        r = redis.from_url(
            REDIS_URL, 
            decode_responses=True, 
            ssl_cert_reqs=None
        )
        
        # Verify connection
        r.ping()
        
        count = 0
        total_scanned = 0
        qualified_stocks = []
        
        print(f"Connected to Redis. Scanning for SMA > {SMA_THRESHOLD}...")

        # Use scan_iter to prevent blocking the production Redis
        for key in r.scan_iter(KEY_PATTERN):
            data_raw = r.get(key)
            if not data_raw:
                continue
                
            try:
                # Your code saves a dictionary as JSON
                data = json.loads(data_raw)
                
                # In your provided code, the field name is 'sma'
                sma_value = data.get('sma', 0)
                symbol = data.get('symbol', 'Unknown')
                
                if sma_value > SMA_THRESHOLD:
                    count += 1
                    qualified_stocks.append(f"{symbol} ({sma_value})")
                
                total_scanned += 1
                    
            except (json.JSONDecodeError, TypeError, AttributeError):
                # Skip keys that aren't related to stock data
                continue

        # Output results
        print("-" * 45)
        print(f"MARKET DATA SUMMARY")
        print("-" * 45)
        print(f"Total Stock Keys Scanned: {total_scanned}")
        print(f"Stocks meeting criteria:  {count}")
        print("-" * 45)
        
        if qualified_stocks:
            print("Qualified Stocks (Symbol & SMA):")
            # Print in columns for readability
            for i in range(0, len(qualified_stocks), 3):
                print(", ".join(qualified_stocks[i:i+3]))
        else:
            print("No stocks found meeting the threshold.")
        
        return count

    except redis.ConnectionError as e:
        print(f"Error: Could not connect to Redis. {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    check_sma_count()