import asyncio
import os
import logging
from redis_manager import TradeControl

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FixSync")

async def repair_and_run():
    print("🛠️  Starting Dhan Credential Repair...")
    
    # YOUR ACTUAL DHAN DETAILS
    client_id = "1101693020"
    access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY3MTY1ODc3LCJpYXQiOjE3NjcwNzk0NzcsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTAxNjkzMDIwIn0.MJtAqt0P17iVmIMJrUx07MYSotnVa1wUELjd6sU7bTV8BguyQ87uM_lLDSKKJ0Xjq9hhwA6lhgk0zRmfZlRUzQ"

    # Use the Manager to save correctly (api_key=client_id, api_secret=access_token)
    success = await TradeControl.save_config(client_id, access_token)
    
    if success:
        print("✅ Redis Keys Repaired successfully.")
        print(f"   Stored {client_id} -> nexus:config:api_key")
        print("   Stored Token -> nexus:config:api_secret")
        
        # Verify
        k, s = await TradeControl.get_config()
        if k == client_id and len(s) > 10:
            print("🚀 Verification Passed! Starting Market Data Sync...")
            
            # Now trigger the sync script logic
            # Instead of importing it, we will run it via OS to ensure a clean environment
            os.system("python3 sync_market_data.py")
        else:
            print("❌ Verification Failed. Keys were not retrieved correctly.")
    else:
        print("❌ Failed to write to Redis. Is Redis running?")

if __name__ == "__main__":
    asyncio.run(repair_and_run())