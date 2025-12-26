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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MAIN] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Nexus_Main")
IST = pytz.timezone("Asia/Kolkata")

app = FastAPI(title="Nexus Core", version="2.5.0", strict_slashes=False)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# RAM STATE (Latched for Parallelism)
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
    "config": {"bull": {}, "bear": {}, "mom_bull": {}, "mom_bear": {}},
    "data_connected": {"breakout": False, "momentum": False},

    # ---- NEW Parallel Infra ----
    "token_locks": defaultdict(asyncio.Lock),
    "engine_sem": asyncio.Semaphore(500), # Limits concurrent engine instances
    "inflight": set(),                    # Tracks active tasks
    "max_inflight": 5000,                 # Backpressure limit
    "candle_close_queue": None,           # Decoupled strategy logic
    "tick_batches_dropped": 0,
}

# -----------------------------
# HELPERS
# -----------------------------
def _now_ist() -> datetime:
    return datetime.now(IST)

def _compute_pnl() -> Dict[str, float]:
    pnl = {k: 0.0 for k in ["bull", "bear", "mom_bull", "mom_bear"]}
    for side in pnl:
        pnl[side] = float(sum(float(t.get("pnl", 0.0) or 0.0) for t in RAM_STATE["trades"].get(side, [])))
    pnl["total"] = float(sum(pnl[s] for s in ["bull", "bear", "mom_bull", "mom_bear"]))
    return pnl

# -----------------------------
# LATENCY FIX: CENTRALIZED CANDLE AGGREGATION
# -----------------------------
def _update_1m_candle(stock: dict, ltp: float, cum_vol: int) -> Optional[dict]:
    """Decoupled from engine logic to ensure tick hot-path is never blocked."""
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
# LATENCY FIX: PARALLEL TICK PROCESSING
# -----------------------------
async def _process_tick_task(token: int, ltp: float, cum_vol: int):
    """Execution logic for a single tick, run in parallel."""
    sem = RAM_STATE["engine_sem"]
    lock = RAM_STATE["token_locks"][token]

    async with sem:
        async with lock:
            stock = RAM_STATE["stocks"].get(token)
            if not stock: return
            stock["ltp"] = ltp

            # 1. Update Candle (Fast)
            closed = _update_1m_candle(stock, ltp, cum_vol)
            if closed:
                # Offload to candle worker so tick processing doesn't wait for strategy math
                RAM_STATE["candle_close_queue"].put_nowait((token, closed))

            # 2. Run Engines (Parallel)
            await asyncio.gather(
                BreakoutEngine.run(token, ltp, cum_vol, RAM_STATE),
                MomentumEngine.run(token, ltp, cum_vol, RAM_STATE),
                return_exceptions=True
            )

async def tick_worker_parallel():
    """Consumes batches and spawns individual tasks for parallel execution."""
    q = RAM_STATE["tick_queue"]
    inflight = RAM_STATE["inflight"]
    max_inflight = RAM_STATE["max_inflight"]

    while True:
        ticks = await q.get()
        try:
            # Backpressure: If we have too many active tasks, wait for some to finish
            if len(inflight) >= max_inflight:
                done, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
                inflight.difference_update(done)

            for tick in ticks:
                token = tick.get("instrument_token")
                ltp = tick.get("last_price")
                vol = tick.get("volume_traded")
                if not token or not ltp: continue

                # Spawn parallel task
                task = asyncio.create_task(_process_tick_task(token, ltp, vol))
                inflight.add(task)
                task.add_done_callback(inflight.discard)

        except Exception as e:
            logger.error(f"Tick Worker Error: {e}")
        finally:
            q.task_done()

async def candle_worker():
    """Handles heavy strategy calculations when a 1m candle closes."""
    cq = RAM_STATE["candle_close_queue"]
    while True:
        token, candle = await cq.get()
        try:
            # We use the same per-token lock to ensure state safety
            async with RAM_STATE["token_locks"][token]:
                await asyncio.gather(
                    BreakoutEngine.on_candle_close(token, candle, RAM_STATE),
                    MomentumEngine.on_candle_close(token, candle, RAM_STATE),
                    return_exceptions=True
                )
        finally:
            cq.task_done()

# -----------------------------
# KITE CALLBACKS
# -----------------------------
def on_ticks(ws, ticks):
    loop = RAM_STATE.get("main_loop")
    q = RAM_STATE.get("tick_queue")
    if loop and q:
        def _put():
            try:
                q.put_nowait(ticks)
            except asyncio.QueueFull:
                try: # Drop oldest to keep system current (Fast Lane)
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
        RAM_STATE["data_connected"] = {"breakout": True, "momentum": True}

# -----------------------------
# LIFECYCLE
# -----------------------------
@app.on_event("startup")
async def startup_event():
    RAM_STATE["main_loop"] = asyncio.get_running_loop()
    RAM_STATE["tick_queue"] = asyncio.Queue(maxsize=5000)
    RAM_STATE["candle_close_queue"] = asyncio.Queue(maxsize=1000)
    
    # Start specialized workers
    asyncio.create_task(tick_worker_parallel())
    asyncio.create_task(candle_worker())

    # Load Data from Redis
    api_key, api_secret = await TradeControl.get_config()
    token = await TradeControl.get_access_token()
    RAM_STATE.update({"api_key": api_key, "api_secret": api_secret, "access_token": token})

    market_data = await TradeControl.get_all_market_data()
    for t, data in market_data.items():
        RAM_STATE["stocks"][int(t)] = {**data, "ltp": 0, "candle_1m": None, "brk_status": "WAITING", "mom_status": "WAITING"}

    # Start KiteTicker
    if RAM_STATE["api_key"] and RAM_STATE["access_token"]:
        kws = KiteTicker(RAM_STATE["api_key"], RAM_STATE["access_token"])
        kws.on_ticks, kws.on_connect = on_ticks, on_connect
        kws.connect(threaded=True)
        RAM_STATE["kws"] = kws

@app.get("/api/stats")
async def get_stats():
    return {
        "pnl": _compute_pnl(),
        "queue_lag": RAM_STATE["tick_queue"].qsize(),
        "inflight_tasks": len(RAM_STATE["inflight"]),
        "dropped_batches": RAM_STATE["tick_batches_dropped"],
        "server_time": _now_ist().strftime("%H:%M:%S")
    }

@app.get("/")
async def home(): return FileResponse("index.html")