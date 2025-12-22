import asyncio
import os
import logging
import threading
import json
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Optional

# Core FastAPI & Server
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Kite Connect SDK
from kiteconnect import KiteConnect, KiteTicker

# --- THE CRITICAL FIX: TWISTED SIGNAL BYPASS ---
# This block MUST run before any KiteTicker connection is established.
# It patches the Twisted reactor to prevent it from trying to catch
# OS signals (SIGTERM/SIGINT) which can only be done in the main thread.
from twisted.internet import reactor
_original_run = reactor.run
def _patched_reactor_run(*args, **kwargs):
    kwargs['installSignalHandlers'] = False
    return _original_run(*args, **kwargs)
reactor.run = _patched_reactor_run
# ----------------------------------------------

# Optimized Event Loop (C-based) for High-Frequency I/O
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

# Custom Async Logic & Data Managers
from breakout_engine import BreakoutEngine
from momentum_engine import MomentumEngine
from redis_manager import TradeControl

# --- DETAILED LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Nexus_Async_Core")
IST = pytz.timezone("Asia/Kolkata")

# --- FASTAPI APP CONFIG ---
app = FastAPI(strict_slashes=False)
app.add_middleware(
    CORSMiddleware, 
    allow_origins=["*"], 
    allow_methods=["*"], 
    allow_headers=["*"]
)

# --- STOCK UNIVERSE ---
# [USER: PASTE YOUR 500+ STOCK_INDEX_MAPPING DICTIONARY HERE]
STOCK_INDEX_MAPPING = {
'MINDTECK': 'NIFTY 500',
'NITIRAJ': 'NIFTY 500',
} 

# --- CONSOLIDATED ASYNC RAM STATE ---
RAM_STATE = {
    "kite": None,
    "kws": None,
    "api_key": "",
    "api_secret": "",
    "access_token": "",
    "stocks": {}, 
    "trades": {side: [] for side in ["bull", "bear", "mom_bull", "mom_bear"]},
    "config": {
        side: {
            "volume_criteria": [{"min_vol_price_cr": 0, "sma_multiplier": 1.0, "min_sma_avg": 0} for _ in range(10)],
            "total_trades": 5, 
            "risk_trade_1": 2000, 
            "risk_reward": "1:2", 
            "trailing_sl": "1:1.5"
        } for side in ["bull", "bear", "mom_bull", "mom_bear"]
    },
    "engine_live": {side: False for side in ["bull", "bear", "mom_bull", "mom_bear"]},
    "pnl": {"total": 0.0, "bull": 0.0, "bear": 0.0, "mom_bull": 0.0, "mom_bear": 0.0},
    "data_connected": {"breakout": False, "momentum": False},
    "manual_exits": set()
}

# --- ASYNC BRIDGE: TICKER TO FASTAPI EVENT LOOP ---
def on_ticks(ws, ticks):
    """
    Bridge that receives ticks from the Twisted thread and injects 
    them into the FastAPI Async Event Loop for superfast processing.
    """
    loop = asyncio.get_event_loop()
    for tick in ticks:
        token = tick['instrument_token']
        if token in RAM_STATE["stocks"]:
            ltp = tick['last_price']
            vol = tick.get('volume_traded', 0)
            
            # Non-blocking RAM update for fast reading by the UI
            RAM_STATE["stocks"][token]['ltp'] = ltp
            
            # Schedule the heavy processing logic in the main async loop
            asyncio.run_coroutine_threadsafe(
                BreakoutEngine.run(token, ltp, vol, RAM_STATE),
                loop
            )

def on_connect(ws, response):
    logger.info("✅ TICKER: Handshake successful. Subscribing to tokens...")
    tokens = list(RAM_STATE["stocks"].keys())
    if tokens:
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)
        logger.info(f"✅ TICKER: Subscribed to {len(tokens)} tokens in FULL mode.")
    RAM_STATE["data_connected"]["breakout"] = True
    RAM_STATE["data_connected"]["momentum"] = True

def on_error(ws, code, reason):
    logger.error(f"❌ TICKER ERROR: {code} - {reason}")

def on_close(ws, code, reason):
    logger.warning(f"⚠️ TICKER: Connection closed ({code}: {reason})")
    RAM_STATE["data_connected"]["breakout"] = False
    RAM_STATE["data_connected"]["momentum"] = False

# --- LIFECYCLE: ASYNC SYSTEM STARTUP ---

@app.on_event("startup")
async def startup_event():
    logger.info("--- 🚀 NEXUS ASYNC ENGINE BOOTING ---")
    
    # 1. Restore API Credentials from Redis
    key, secret = await TradeControl.get_config()
    token = await TradeControl.get_access_token()
    
    if key and secret:
        RAM_STATE["api_key"], RAM_STATE["api_secret"] = key, secret
        logger.info(f"🔑 REDIS: API Credentials Restored ({key[:4]}***)")

    if token and key:
        try:
            RAM_STATE["access_token"] = token
            RAM_STATE["kite"] = KiteConnect(api_key=key)
            RAM_STATE["kite"].set_access_token(token)
            
            # 2. Map Instruments (Run blocking call in executor)
            logger.info("📡 KITE: Mapping NSE Instruments...")
            instruments = await asyncio.to_thread(RAM_STATE["kite"].instruments, "NSE")
            for instr in instruments:
                symbol = instr['tradingsymbol']
                if symbol in STOCK_INDEX_MAPPING:
                    t_id = instr['instrument_token']
                    RAM_STATE["stocks"][t_id] = {
                        'symbol': symbol, 'ltp': 0, 'status': 'WAITING', 'trades': 0,
                        'hi': 0, 'lo': 0, 'pdh': 0, 'pdl': 0, 'sma': 0, 'candle': None, 'last_vol': 0
                    }
            
            # 3. Fast-Boot: Hydrate Stock SMA/PDH from Redis Cache
            cached_data = await TradeControl.get_all_market_data()
            if cached_data:
                logger.info(f"⚡ CACHE: Hydrating {len(cached_data)} stocks from market cache.")
                for t_id_str, data in cached_data.items():
                    t_id = int(t_id_str)
                    if t_id in RAM_STATE["stocks"]:
                        RAM_STATE["stocks"][t_id].update(data)
            
            # 4. Start Ticker with Patched Reactor
            # We use threaded=True to ensure it doesn't block the main thread
            RAM_STATE["kws"] = KiteTicker(key, token)
            RAM_STATE["kws"].on_ticks = on_ticks
            RAM_STATE["kws"].on_connect = on_connect
            RAM_STATE["kws"].on_error = on_error
            RAM_STATE["kws"].on_close = on_close
            
            # Start Ticker. This will now bypass signal installation.
            RAM_STATE["kws"].connect(threaded=True)
            logger.info("🛰️ SYSTEM: Ticker initialized in background thread (Signal-Safe).")

        except Exception as e:
            logger.error(f"❌ STARTUP CRASH: {e}")

# --- DASHBOARD & API ENDPOINTS ---

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    with open("index.html", "r") as f: return f.read()

@app.get("/api/stats")
async def get_stats():
    total_pnl = 0.0
    engine_stats = {}
    for side in ["bull", "bear", "mom_bull", "mom_bear"]:
        side_pnl = sum(t.get('pnl', 0) for t in RAM_STATE["trades"][side])
        engine_stats[side] = side_pnl
        total_pnl += side_pnl
    
    RAM_STATE["pnl"]["total"] = total_pnl
    return {
        "pnl": {**RAM_STATE["pnl"], **engine_stats},
        "data_connected": RAM_STATE["data_connected"],
        "engine_status": {k: ("1" if v else "0") for k, v in RAM_STATE["engine_live"].items()}
    }

@app.get("/api/orders")
async def get_orders(): return RAM_STATE["trades"]

@app.get("/api/scanner")
async def get_scanner():
    signals = {side: [] for side in ["bull", "bear", "mom_bull", "mom_bear"]}
    for t_id, s in RAM_STATE["stocks"].items():
        if s.get('status') in ['TRIGGER_WATCH', 'MOM_TRIGGER_WATCH']:
            side = s.get('side_latch', '').lower()
            if side in signals:
                signals[side].append({"symbol": s['symbol'], "price": s['trigger_px']})
    return signals

@app.post("/api/control")
async def control_center(data: dict):
    action = data.get("action")
    if action == "save_api":
        key, secret = data.get("api_key"), data.get("api_secret")
        RAM_STATE["api_key"], RAM_STATE["api_secret"] = key, secret
        await TradeControl.save_config(key, secret)
        logger.info("💾 CONTROL: API credentials persisted.")
    elif action == "get_saved_keys":
        return {"api_key": RAM_STATE["api_key"], "api_secret": "********" if RAM_STATE["api_secret"] else ""}
    elif action == "toggle_engine":
        RAM_STATE["engine_live"][data['side']] = data['enabled']
        logger.info(f"⚙️ CONTROL: Engine {data['side']} toggled to {data['enabled']}")
    elif action == "manual_exit":
        RAM_STATE["manual_exits"].add(data['symbol'])
    return {"status": "ok"}

@app.get("/api/settings/engine/{side}")
async def get_engine_settings(side: str):
    return RAM_STATE["config"].get(side, {})

@app.post("/api/settings/engine/{side}")
async def save_engine_settings(side: str, data: dict):
    RAM_STATE["config"][side].update(data)
    logger.info(f"📝 SETTINGS: Volume matrix updated for {side}.")
    return {"status": "success"}

@app.get("/api/kite/login")
async def kite_login_redirect():
    api_key = RAM_STATE["api_key"] or os.getenv("KITE_API_KEY")
    if not api_key: return {"status": "error", "message": "Save API Key first."}
    return RedirectResponse(url=KiteConnect(api_key=api_key).login_url())

@app.get("/login")
async def kite_callback(request_token: str = None):
    try:
        api_key, api_secret = RAM_STATE["api_key"], RAM_STATE["api_secret"]
        kite = KiteConnect(api_key=api_key)
        data = await asyncio.to_thread(kite.generate_session, request_token, api_secret=api_secret)
        
        token = data["access_token"]
        await TradeControl.save_access_token(token)
        logger.info("🔑 AUTH: Zerodha Login successful. Token persisted.")
        
        return RedirectResponse(url="/")
    except Exception as e:
        logger.error(f"❌ AUTH ERROR: {e}")
        return {"status": "error", "message": str(e)}

# --- SERVER ENTRY POINT ---

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    # Crucial: Use uvloop for the high-performance async ticker bridge
    uvicorn.run("main:app", host="0.0.0.0", port=port, loop="uvloop", workers=1)