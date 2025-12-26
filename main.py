import asyncio
import os
import logging
import json
import time
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
from collections import defaultdict

import pytz
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

from kiteconnect import KiteConnect, KiteTicker

from redis_manager import TradeControl
from breakout_engine import BreakoutEngine
from momentum_engine import MomentumEngine

# -----------------------------
# ✅ HEROKU / GUNICORN TWISTED SIGNAL BYPASS
# -----------------------------
try:
    from twisted.internet import reactor  # type: ignore
    _original_run = reactor.run

    def _patched_reactor_run(*args, **kwargs):
        kwargs["installSignalHandlers"] = False
        return _original_run(*args, **kwargs)

    reactor.run = _patched_reactor_run
except Exception:
    pass

# -----------------------------
# LOGGING / TIMEZONE
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MAIN] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Nexus_Main")
IST = pytz.timezone("Asia/Kolkata")

# -----------------------------
# FASTAPI APP
# -----------------------------
app = FastAPI(title="Nexus Core", version="2.7.0", strict_slashes=False)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# DEFAULTS
# -----------------------------
def _default_volume_matrix() -> list:
    multipliers = [20, 18, 16, 14, 12, 10, 8, 6, 4, 2]
    out = []
    for i in range(10):
        out.append({
            "min_vol_price_cr": float(i + 1),
            "sma_multiplier": float(multipliers[i]),
            "min_sma_avg": int(1000),
        })
    return out

DEFAULT_CONFIG: Dict[str, Dict[str, Any]] = {
    "bull": {"risk_reward": "1:2", "trailing_sl": "1:1.5", "total_trades": 5, "risk_trade_1": 2000, "volume_criteria": _default_volume_matrix(), "trade_start": "09:15", "trade_end": "15:10"},
    "bear": {"risk_reward": "1:2", "trailing_sl": "1:1.5", "total_trades": 5, "risk_trade_1": 2000, "volume_criteria": _default_volume_matrix(), "trade_start": "09:15", "trade_end": "15:10"},
    "mom_bull": {"risk_reward": "1:2", "trailing_sl": "1:1.5", "total_trades": 5, "risk_trade_1": 2000, "volume_criteria": _default_volume_matrix(), "trade_start": "09:15", "trade_end": "09:17"},
    "mom_bear": {"risk_reward": "1:2", "trailing_sl": "1:1.5", "total_trades": 5, "risk_trade_1": 2000, "volume_criteria": _default_volume_matrix(), "trade_start": "09:15", "trade_end": "09:17"},
}

# -----------------------------
# GLOBAL RAM STATE
# -----------------------------
RAM_STATE: Dict[str, Any] = {
    "main_loop": None,
    "tick_queue": None,
    "kite": None,
    "kws": None,
    "api_key": "",
    "api_secret": "",
    "access_token": "",
    "stocks": {},
    "trades": {"bull": [], "bear": [], "mom_bull": [], "mom_bear": []},
    "engine_live": {"bull": True, "bear": True, "mom_bull": True, "mom_bear": True},
    "config": {k: dict(v) for k, v in DEFAULT_CONFIG.items()},
    "manual_exits": set(),
    "data_connected": {"breakout": False, "momentum": False},

    # Parallel Processing Infrastructure
    "token_locks": defaultdict(asyncio.Lock),
    "engine_sem": None, 
    "inflight": set(),
    "max_inflight": 5000,
    "candle_close_queue": None,
    "tick_batches_dropped": 0,
    "tick_batches_enqueued": 0,
}

# -----------------------------
# HELPERS
# -----------------------------
def _now_ist() -> datetime:
    return datetime.now(IST)

def _safe_float(x, default=0.0) -> float:
    try: return float(x)
    except: return default

def _safe_int(x, default=0) -> int:
    try: return int(x)
    except: return default

def _compute_pnl() -> Dict[str, float]:
    pnl = {k: 0.0 for k in ["bull", "bear", "mom_bull", "mom_bear"]}
    for side in pnl:
        pnl[side] = float(sum(float(t.get("pnl", 0.0) or 0.0) for t in RAM_STATE["trades"].get(side, [])))
    pnl["total"] = float(sum(pnl[s] for s in ["bull", "bear", "mom_bull", "mom_bear"]))
    return pnl

def _trade_is_open(t: dict) -> bool:
    return str(t.get("status", "OPEN")).upper() == "OPEN"

# -----------------------------
# LATENCY FIX: CENTRALIZED CANDLE AGGREGATOR
# -----------------------------
def _update_1m_candle(stock: dict, ltp: float, cum_vol: int) -> Optional[dict]:
    now = _now_ist()
    bucket = now.replace(second=0, microsecond=0)
    c = stock.get("candle_1m")
    last_cum = int(stock.get("candle_last_cum_vol", 0) or 0)

    if not c:
        stock["candle_1m"] = {"bucket": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": 0}
        stock["candle_last_cum_vol"] = cum_vol
        return None

    if c["bucket"] != bucket:
        closed = dict(c)
        stock["candle_1m"] = {"bucket": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": 0}
        stock["candle_last_cum_vol"] = cum_vol
        return closed

    c["high"] = max(c["high"], ltp)
    c["low"] = min(c["low"], ltp)
    c["close"] = ltp
    if last_cum > 0:
        c["volume"] += max(0, cum_vol - last_cum)
    stock["candle_last_cum_vol"] = cum_vol
    return None

# -----------------------------
# LATENCY FIX: PARALLEL TICK PIPELINE
# -----------------------------
async def _process_tick_task(token: int, ltp: float, vol: int):
    sem = RAM_STATE["engine_sem"]
    lock = RAM_STATE["token_locks"][token]
    async with sem:
        async with lock:
            stock = RAM_STATE["stocks"].get(token)
            if not stock: return
            stock["ltp"] = ltp
            stock["last_update_ts"] = time.time()

            # 1. Update Candle (Fast)
            closed = _update_1m_candle(stock, ltp, vol)
            if closed:
                RAM_STATE["candle_close_queue"].put_nowait((token, closed))

            # 2. Parallel Engine Run
            await asyncio.gather(
                BreakoutEngine.run(token, ltp, vol, RAM_STATE),
                MomentumEngine.run(token, ltp, vol, RAM_STATE),
                return_exceptions=True
            )

async def tick_worker_parallel():
    q = RAM_STATE["tick_queue"]
    inflight = RAM_STATE["inflight"]
    logger.info("🧵 Parallel Tick Worker: Active")
    while True:
        ticks = await q.get()
        try:
            if len(inflight) >= RAM_STATE["max_inflight"]:
                done, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
                inflight.difference_update(done)

            for tick in ticks:
                token = tick.get("instrument_token")
                if not token: continue
                task = asyncio.create_task(_process_tick_task(token, tick.get("last_price", 0), tick.get("volume_traded", 0)))
                inflight.add(task)
                task.add_done_callback(inflight.discard)
        finally:
            q.task_done()

async def candle_worker():
    cq = RAM_STATE["candle_close_queue"]
    logger.info("🕯️ Background Candle Worker: Active")
    while True:
        token, candle = await cq.get()
        try:
            async with RAM_STATE["token_locks"][token]:
                await asyncio.gather(
                    BreakoutEngine.on_candle_close(token, candle, RAM_STATE),
                    MomentumEngine.on_candle_close(token, candle, RAM_STATE),
                    return_exceptions=True
                )
        finally:
            cq.task_done()

# -----------------------------
# KITE WebSocket Handlers
# -----------------------------
def on_ticks(ws, ticks):
    loop = RAM_STATE.get("main_loop")
    q = RAM_STATE.get("tick_queue")
    if loop and q:
        def _put():
            try:
                q.put_nowait(ticks)
                RAM_STATE["tick_batches_enqueued"] += 1
            except asyncio.QueueFull:
                try:
                    q.get_nowait()
                    q.put_nowait(ticks)
                    RAM_STATE["tick_batches_dropped"] += 1
                except: pass
        loop.call_soon_threadsafe(_put)

def on_connect(ws, response):
    tokens = list(RAM_STATE["stocks"].keys())
    if tokens:
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)
        logger.info(f"📡 WS Connected: Subscribed {len(tokens)} tokens")
        RAM_STATE["data_connected"] = {"breakout": True, "momentum": True}

# -----------------------------
# FASTAPI ROUTES (Full Set)
# -----------------------------
@app.get("/", response_class=HTMLResponse)
async def home():
    try: return FileResponse("index.html")
    except: return HTMLResponse("<h3>Dashboard file missing</h3>")

@app.get("/login")
@app.get("/login/")
async def login(request_token: Optional[str] = Query(None)):
    api_key = RAM_STATE["api_key"] or os.getenv("KITE_API_KEY")
    api_secret = RAM_STATE["api_secret"] or os.getenv("KITE_API_SECRET")
    if not request_token:
        return RedirectResponse(KiteConnect(api_key=api_key).login_url())
    try:
        kite = KiteConnect(api_key=api_key)
        data = await asyncio.to_thread(kite.generate_session, request_token, api_secret=api_secret)
        await TradeControl.save_access_token(data["access_token"])
        RAM_STATE["access_token"] = data["access_token"]
        RAM_STATE["kite"] = kite
        RAM_STATE["kite"].set_access_token(data["access_token"])
        logger.info("✅ Login Success")
        return RedirectResponse(url="/")
    except Exception as e:
        return HTMLResponse(f"<h3>Login Failed: {e}</h3>")

@app.get("/api/stats")
async def get_stats():
    return {
        "pnl": _compute_pnl(),
        "queue": RAM_STATE["tick_queue"].qsize() if RAM_STATE["tick_queue"] else 0,
        "inflight": len(RAM_STATE["inflight"]),
        "dropped": RAM_STATE["tick_batches_dropped"],
        "server_time": _now_ist().strftime("%H:%M:%S"),
        "engine_status": {k: "1" if v else "0" for k, v in RAM_STATE["engine_live"].items()},
        "data_connected": RAM_STATE["data_connected"]
    }

@app.get("/api/orders")
async def get_orders(open_only: int = 0):
    if not open_only: return RAM_STATE["trades"]
    return {side: [t for t in arr if _trade_is_open(t)] for side, arr in RAM_STATE["trades"].items()}

@app.get("/api/scanner")
async def get_scanner():
    signals = {side: [] for side in ["bull", "bear", "mom_bull", "mom_bear"]}
    for _, s in RAM_STATE["stocks"].items():
        for p in ["brk", "mom"]:
            if s.get(f"{p}_status") == "TRIGGER_WATCH":
                side = s.get(f"{p}_side_latch")
                if side in signals:
                    signals[side].append({
                        "symbol": s["symbol"],
                        "trigger_px": s.get(f"{p}_trigger_px"),
                        "seen_time": s.get(f"{p}_scan_seen_time") or _now_ist().strftime("%H:%M:%S")
                    })
    return signals

@app.get("/api/settings/engine/{side}")
async def get_settings(side: str):
    return RAM_STATE["config"].get(side, {})

@app.post("/api/settings/engine/{side}")
async def save_settings(side: str, data: Dict[str, Any]):
    if side in RAM_STATE["config"]:
        RAM_STATE["config"][side].update(data)
        await TradeControl.save_strategy_settings(side, RAM_STATE["config"][side])
    return {"status": "success"}

@app.post("/api/control")
async def control(request: Request):
    data = await request.json()
    action = data.get("action")
    if action == "toggle_engine":
        side, enabled = data.get("side"), data.get("enabled")
        if side in RAM_STATE["engine_live"]: RAM_STATE["engine_live"][side] = bool(enabled)
    elif action == "square_off_one":
        symbol, side = data.get("symbol"), data.get("side")
        stock = next((s for s in RAM_STATE["stocks"].values() if s["symbol"] == symbol), None)
        if stock:
            if "mom" in side: await MomentumEngine.close_position(stock, RAM_STATE, "USER_EXIT")
            else: await BreakoutEngine.close_position(stock, RAM_STATE, "USER_EXIT")
    return {"status": "ok"}

# -----------------------------
# STARTUP EVENT
# -----------------------------
@app.on_event("startup")
async def startup_event():
    logger.info("🚀 System Startup")
    RAM_STATE["main_loop"] = asyncio.get_running_loop()
    RAM_STATE["tick_queue"] = asyncio.Queue(maxsize=10000)
    RAM_STATE["candle_close_queue"] = asyncio.Queue(maxsize=2000)
    RAM_STATE["engine_sem"] = asyncio.Semaphore(500)
    
    asyncio.create_task(tick_worker_parallel())
    asyncio.create_task(candle_worker())

    # Auth & Config Restoration
    api_key, api_secret = await TradeControl.get_config()
    token = await TradeControl.get_access_token()
    RAM_STATE.update({"api_key": api_key, "api_secret": api_secret, "access_token": token})

    # Load Universe
    market_data = await TradeControl.get_all_market_data()
    for t_str, data in market_data.items():
        t_id = int(t_str)
        RAM_STATE["stocks"][t_id] = {**data, "token": t_id, "ltp": 0.0, "brk_status": "WAITING", "mom_status": "WAITING", "candle_1m": None}

    # Connect WebSocket
    if RAM_STATE["api_key"] and RAM_STATE["access_token"]:
        try:
            kws = KiteTicker(RAM_STATE["api_key"], RAM_STATE["access_token"])
            kws.on_ticks, kws.on_connect = on_ticks, on_connect
            kws.connect(threaded=True)
            RAM_STATE["kws"] = kws
        except Exception as e:
            logger.error(f"WS Error: {e}")

@app.on_event("shutdown")
async def shutdown():
    if RAM_STATE["kws"]: RAM_STATE["kws"].close()