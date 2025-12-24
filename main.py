import asyncio
import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import pytz
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

from kiteconnect import KiteConnect, KiteTicker

from redis_manager import TradeControl
from breakout_engine import BreakoutEngine
from momentum_engine import MomentumEngine

# Twisted signal bypass for gunicorn
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

# ✅ FastAPI syntax fix: redirect_slashes (not strict_slashes)
app = FastAPI(title="Nexus Core", version="1.0.0", redirect_slashes=False)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _default_volume_matrix() -> list:
    multipliers = [20, 18, 16, 14, 12, 10, 8, 6, 4, 2]
    out = []
    for i in range(10):
        out.append(
            {
                "min_vol_price_cr": float(i + 1),
                "sma_multiplier": float(multipliers[i]),
                "min_sma_avg": int(1000),
            }
        )
    return out

DEFAULT_CONFIG: Dict[str, Dict[str, Any]] = {
    "bull": {"risk_reward":"1:2","trailing_sl":"1:1.5","total_trades":5,"risk_trade_1":2000,"volume_criteria":_default_volume_matrix(),"trade_start":"09:15","trade_end":"15:10"},
    "bear": {"risk_reward":"1:2","trailing_sl":"1:1.5","total_trades":5,"risk_trade_1":2000,"volume_criteria":_default_volume_matrix(),"trade_start":"09:15","trade_end":"15:10"},
    "mom_bull": {"risk_reward":"1:2","trailing_sl":"1:1.5","total_trades":5,"risk_trade_1":2000,"volume_criteria":_default_volume_matrix(),"trade_start":"09:15","trade_end":"09:17"},
    "mom_bear": {"risk_reward":"1:2","trailing_sl":"1:1.5","total_trades":5,"risk_trade_1":2000,"volume_criteria":_default_volume_matrix(),"trade_start":"09:15","trade_end":"09:17"},
}

RAM_STATE: Dict[str, Any] = {
    "main_loop": None,
    "tick_queue": None,
    "tick_worker_task": None,
    "kite": None,
    "kws": None,
    "api_key": "",
    "api_secret": "",
    "access_token": "",
    "stocks": {},
    "trades": {"bull": [], "bear": [], "mom_bull": [], "mom_bear": []},
    "engine_live": {"bull": True, "bear": True, "mom_bull": True, "mom_bear": True},
    "config": {
        "bull": dict(DEFAULT_CONFIG["bull"]),
        "bear": dict(DEFAULT_CONFIG["bear"]),
        "mom_bull": dict(DEFAULT_CONFIG["mom_bull"]),
        "mom_bear": dict(DEFAULT_CONFIG["mom_bear"]),
    },
    "manual_exits": set(),
    "data_connected": {"breakout": False, "momentum": False},
}

def _now_ist() -> datetime:
    return datetime.now(IST)

def _safe_int(x, default=0) -> int:
    try: return int(x)
    except Exception: return default

def _safe_float(x, default=0.0) -> float:
    try: return float(x)
    except Exception: return default

def _compute_pnl() -> Dict[str, float]:
    pnl = {k: 0.0 for k in ["bull","bear","mom_bull","mom_bear"]}
    for side in pnl.keys():
        pnl[side] = float(sum(float(t.get("pnl", 0.0) or 0.0) for t in RAM_STATE["trades"].get(side, [])))
    pnl["total"] = float(sum(pnl[s] for s in ["bull","bear","mom_bull","mom_bear"]))
    return pnl

# -----------------------------
# CENTRALIZED CANDLE AGGREGATION
# -----------------------------
def _update_candle(stock: dict, ltp: float, vol: int):
    now = datetime.now(IST)
    bucket = now.replace(second=0, microsecond=0)

    c = stock.get("candle")
    if not c:
        stock["candle"] = {
            "bucket": bucket,
            "open": float(ltp),
            "high": float(ltp),
            "low": float(ltp),
            "close": float(ltp),
            "volume": 0,
        }
        stock["last_vol"] = int(vol)
        return None  # no closed candle

    if c["bucket"] != bucket:
        closed = c
        stock["candle"] = {
            "bucket": bucket,
            "open": float(ltp),
            "high": float(ltp),
            "low": float(ltp),
            "close": float(ltp),
            "volume": 0,
        }
        stock["last_vol"] = int(vol)
        return closed

    # same candle
    c["high"] = max(float(c["high"]), float(ltp))
    c["low"] = min(float(c["low"]), float(ltp))
    c["close"] = float(ltp)

    last_vol = int(stock.get("last_vol", 0) or 0)
    if last_vol > 0:
        c["volume"] += max(0, int(vol) - last_vol)
    stock["last_vol"] = int(vol)
    return None

# -----------------------------
# TICK WORKER
# -----------------------------
async def tick_worker():
    q: asyncio.Queue = RAM_STATE["tick_queue"]
    logger.info("🧵 tick_worker running")

    while True:
        ticks = await q.get()
        try:
            for tick in ticks:
                token = _safe_int(tick.get("instrument_token", 0), 0)
                if token <= 0:
                    continue

                stock = RAM_STATE["stocks"].get(token)
                if not stock:
                    continue

                ltp = _safe_float(tick.get("last_price", 0.0), 0.0)
                vol = _safe_int(tick.get("volume_traded", 0), 0)
                if ltp <= 0:
                    continue

                stock["ltp"] = ltp

                # ✅ centralized candle aggregation (always)
                closed = _update_candle(stock, ltp, vol)
                if closed:
                    # pass same closed candle to both engines
                    asyncio.create_task(BreakoutEngine.on_candle_close(token, closed, RAM_STATE))
                    asyncio.create_task(MomentumEngine.on_candle_close(token, closed, RAM_STATE))

                # ✅ always run both engines (they self-gate entries by toggle)
                await BreakoutEngine.run(token, ltp, vol, RAM_STATE)
                await MomentumEngine.run(token, ltp, vol, RAM_STATE)

            RAM_STATE["data_connected"]["breakout"] = True
            RAM_STATE["data_connected"]["momentum"] = True

        except Exception:
            logger.exception("❌ tick_worker crashed while processing ticks")
        finally:
            q.task_done()

def on_ticks(ws, ticks):
    loop = RAM_STATE.get("main_loop")
    q = RAM_STATE.get("tick_queue")
    if not loop or not q:
        return

    def _put():
        try:
            q.put_nowait(ticks)
        except asyncio.QueueFull:
            try:
                _ = q.get_nowait()
                q.task_done()
            except Exception:
                pass
            try:
                q.put_nowait(ticks)
            except Exception:
                pass

    loop.call_soon_threadsafe(_put)

def on_connect(ws, response):
    logger.info("✅ TICKER: Connected. Subscribing to universe tokens...")

    async def _sub():
        tokens = await TradeControl.get_subscribe_universe_tokens()
        tokens = [int(x) for x in tokens if int(x) > 0]
        if not tokens:
            tokens = list(RAM_STATE["stocks"].keys())
            logger.warning(f"⚠️ WS: universe empty; fallback subscribe RAM stocks = {len(tokens)}")

        tokens = [t for t in tokens if t in RAM_STATE["stocks"]]
        if not tokens:
            logger.error("❌ WS: No valid tokens to subscribe (RAM_STATE empty).")
            return

        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)
        logger.info(f"📡 WS: Subscribed {len(tokens)} tokens (MODE_FULL).")

        RAM_STATE["data_connected"]["breakout"] = True
        RAM_STATE["data_connected"]["momentum"] = True

    loop = RAM_STATE.get("main_loop")
    if loop:
        asyncio.run_coroutine_threadsafe(_sub(), loop)

def on_error(ws, code, reason):
    logger.error(f"❌ TICKER ERROR: {code} - {reason}")
    RAM_STATE["data_connected"]["breakout"] = False
    RAM_STATE["data_connected"]["momentum"] = False

def on_close(ws, code, reason):
    logger.warning(f"⚠️ TICKER CLOSED: ({code}) {reason}")
    RAM_STATE["data_connected"]["breakout"] = False
    RAM_STATE["data_connected"]["momentum"] = False

def _start_kiteticker():
    # Close old
    try:
        kws = RAM_STATE.get("kws")
        if kws:
            kws.close()
    except Exception:
        pass

    api_key = RAM_STATE.get("api_key") or ""
    access_token = RAM_STATE.get("access_token") or ""
    if not api_key or not access_token:
        return False

    try:
        kws = KiteTicker(api_key, access_token)
        kws.on_ticks = on_ticks
        kws.on_connect = on_connect
        kws.on_error = on_error
        kws.on_close = on_close
        kws.connect(threaded=True)
        RAM_STATE["kws"] = kws
        logger.info("🛰️ WS: KiteTicker started (threaded=True).")
        return True
    except Exception as e:
        logger.exception(f"❌ WS START FAILED: {e}")
        RAM_STATE["kws"] = None
        return False

@app.on_event("startup")
async def startup_event():
    logger.info("--- 🚀 NEXUS BOOT ---")
    RAM_STATE["main_loop"] = asyncio.get_running_loop()
    RAM_STATE["tick_queue"] = asyncio.Queue(maxsize=2000)
    RAM_STATE["tick_worker_task"] = asyncio.create_task(tick_worker())

    key_redis, sec_redis = await TradeControl.get_config()
    RAM_STATE["api_key"] = str(key_redis or os.getenv("KITE_API_KEY", "") or "")
    RAM_STATE["api_secret"] = str(sec_redis or os.getenv("KITE_API_SECRET", "") or "")

    token = await TradeControl.get_access_token()
    RAM_STATE["access_token"] = str(token or "")

    cached = await TradeControl.get_all_market_data()
    stocks: Dict[int, Dict[str, Any]] = {}

    for token_str, md in (cached or {}).items():
        try:
            t_id = int(token_str)
        except Exception:
            continue

        stocks[t_id] = {
            "token": t_id,
            "symbol": md.get("symbol"),
            "sma": _safe_float(md.get("sma", 0), 0.0),
            "pdh": _safe_float(md.get("pdh", 0), 0.0),
            "pdl": _safe_float(md.get("pdl", 0), 0.0),
            "prev_close": _safe_float(md.get("prev_close", 0), 0.0),
            "sync_time": md.get("sync_time", ""),

            # shared market runtime
            "ltp": 0.0,
            "candle": None,
            "last_vol": 0,

            # breakout engine state
            "brk_status": "WAITING",
            "brk_active_trade": None,
            "brk_side_latch": None,
            "brk_trigger_px": None,
            "brk_trigger_set_ts": None,
            "brk_trigger_candle": None,
            "brk_scan_seen_ts": None,
            "brk_scan_seen_time": None,
            "brk_scan_vol": 0,
            "brk_scan_reason": None,

            # momentum engine state
            "mom_status": "WAITING",
            "mom_active_trade": None,
            "mom_side_latch": None,
            "mom_trigger_px": None,
            "mom_trigger_candle": None,
            "mom_scan_seen_ts": None,
            "mom_scan_seen_time": None,
            "mom_scan_vol": 0,
            "mom_scan_reason": None,
        }

    RAM_STATE["stocks"] = stocks
    logger.info(f"✅ STARTUP: Loaded {len(stocks)} stocks from Redis market_cache.")

    for side in ["bull", "bear", "mom_bull", "mom_bear"]:
        saved = await TradeControl.get_strategy_settings(side)
        if saved:
            RAM_STATE["config"][side].update(saved)

    if RAM_STATE["api_key"] and RAM_STATE["access_token"]:
        try:
            RAM_STATE["kite"] = KiteConnect(api_key=RAM_STATE["api_key"])
            RAM_STATE["kite"].set_access_token(RAM_STATE["access_token"])
        except Exception:
            RAM_STATE["kite"] = None

        _start_kiteticker()
    else:
        logger.warning("⚠️ WS: Not started (missing api_key or access_token).")

@app.on_event("shutdown")
async def shutdown_event():
    try:
        kws = RAM_STATE.get("kws")
        if kws:
            kws.close()
    except Exception:
        pass

@app.get("/", response_class=HTMLResponse)
async def home():
    try:
        return FileResponse("index.html")
    except Exception:
        return HTMLResponse("<h3>Nexus backend is running. Place index.html next to main.py</h3>")

@app.get("/login/")
async def login(request_token: Optional[str] = None):
    api_key = RAM_STATE["api_key"] or os.getenv("KITE_API_KEY", "")
    api_secret = RAM_STATE["api_secret"] or os.getenv("KITE_API_SECRET", "")

    if not api_key:
        k, s = await TradeControl.get_config()
        api_key = api_key or (k or "")
        api_secret = api_secret or (s or "")

    if not api_key:
        return HTMLResponse("<h3>Save API KEY first (API Gateway button)</h3>")

    if not request_token:
        try:
            url = KiteConnect(api_key=str(api_key)).login_url()
            return RedirectResponse(url=url)
        except Exception as e:
            return HTMLResponse(f"<h3>Login URL error: {e}</h3>")

    try:
        kite = KiteConnect(api_key=str(api_key))
        data = await asyncio.to_thread(kite.generate_session, request_token, api_secret=str(api_secret))
        access_token = data["access_token"]

        await TradeControl.save_access_token(access_token)

        RAM_STATE["access_token"] = str(access_token)
        RAM_STATE["api_key"] = str(api_key)
        RAM_STATE["api_secret"] = str(api_secret)

        RAM_STATE["kite"] = kite
        RAM_STATE["kite"].set_access_token(access_token)

        # ✅ Restart WS immediately after login
        _start_kiteticker()

        logger.info("✅ AUTH: Session established & saved to Redis.")
        return RedirectResponse(url="/")
    except Exception as e:
        logger.exception(f"❌ AUTH ERROR: {e}")
        return HTMLResponse(f"<h3>Auth failed: {e}</h3>")

@app.get("/api/stats")
async def get_stats():
    pnl = _compute_pnl()
    return {
        "pnl": pnl,
        "engine_status": {k: "1" if bool(v) else "0" for k, v in RAM_STATE["engine_live"].items()},
        "data_connected": RAM_STATE.get("data_connected", {"breakout": False, "momentum": False}),
        "stock_count": len(RAM_STATE["stocks"]),
        "server_time": _now_ist().strftime("%H:%M:%S"),
    }

@app.get("/api/orders")
async def get_orders():
    return RAM_STATE["trades"]

@app.get("/api/settings/engine/{side}")
async def get_engine_settings(side: str):
    return RAM_STATE["config"].get(side, {})

@app.post("/api/settings/engine/{side}")
async def save_engine_settings(side: str, data: Dict[str, Any]):
    if side in RAM_STATE["config"]:
        RAM_STATE["config"][side].update(data)
        await TradeControl.save_strategy_settings(side, RAM_STATE["config"][side])
    return {"status": "success"}

@app.get("/api/scanner")
async def get_scanner():
    signals = {side: [] for side in ["bull", "bear", "mom_bull", "mom_bear"]}
    now = _now_ist()

    for _, s in RAM_STATE["stocks"].items():
        sym = s.get("symbol") or ""
        if not sym:
            continue

        # breakout scanner
        if s.get("brk_status") == "TRIGGER_WATCH":
            side = (s.get("brk_side_latch") or "").lower()
            if side in ("bull", "bear"):
                if not s.get("brk_scan_seen_ts"):
                    s["brk_scan_seen_ts"] = int(now.timestamp())
                    s["brk_scan_seen_time"] = now.strftime("%H:%M:%S")

                trig = s.get("brk_trigger_px", 0)
                vol = int(s.get("brk_scan_vol", 0) or 0)
                sma = float(s.get("sma", 0) or 0)
                rel = (float(vol) / sma) if sma > 0 else None

                prev_close = float(s.get("prev_close", 0) or 0)
                ltp = float(s.get("ltp", 0) or 0)
                pct = ((ltp - prev_close) / prev_close * 100.0) if prev_close > 0 and ltp > 0 else None

                signals[side].append({
                    "symbol": sym,
                    "trigger_px": trig,
                    "seen_time": s.get("brk_scan_seen_time"),
                    "seen_ts": s.get("brk_scan_seen_ts"),
                    "volume": vol,
                    "rel_vol": rel,
                    "pct_change": pct,
                    "chart_url": f"https://www.tradingview.com/chart/?symbol=NSE:{sym}",
                    "reason": s.get("brk_scan_reason", "Candle Confirmed"),
                })

        # momentum scanner
        if s.get("mom_status") == "TRIGGER_WATCH":
            side = (s.get("mom_side_latch") or "").lower()
            if side in ("mom_bull", "mom_bear"):
                if not s.get("mom_scan_seen_ts"):
                    s["mom_scan_seen_ts"] = int(now.timestamp())
                    s["mom_scan_seen_time"] = now.strftime("%H:%M:%S")

                trig = s.get("mom_trigger_px", 0)
                vol = int(s.get("mom_scan_vol", 0) or 0)
                sma = float(s.get("sma", 0) or 0)
                rel = (float(vol) / sma) if sma > 0 else None

                prev_close = float(s.get("prev_close", 0) or 0)
                ltp = float(s.get("ltp", 0) or 0)
                pct = ((ltp - prev_close) / prev_close * 100.0) if prev_close > 0 and ltp > 0 else None

                signals[side].append({
                    "symbol": sym,
                    "trigger_px": trig,
                    "seen_time": s.get("mom_scan_seen_time"),
                    "seen_ts": s.get("mom_scan_seen_ts"),
                    "volume": vol,
                    "rel_vol": rel,
                    "pct_change": pct,
                    "chart_url": f"https://www.tradingview.com/chart/?symbol=NSE:{sym}",
                    "reason": s.get("mom_scan_reason", "Candle Confirmed"),
                })

    for side in signals:
        signals[side].sort(key=lambda x: x.get("seen_ts", 0), reverse=True)

    return signals

@app.post("/api/control")
async def control(request: Request):
    data = await request.json()
    action = data.get("action")

    if action == "toggle_engine":
        side = data.get("side")
        enabled = bool(data.get("enabled"))
        if side in RAM_STATE["engine_live"]:
            RAM_STATE["engine_live"][side] = enabled
        return {"status": "ok"}

    if action == "save_api":
        api_key = data.get("api_key", "") or ""
        api_secret = data.get("api_secret", "") or ""
        RAM_STATE["api_key"] = str(api_key)
        RAM_STATE["api_secret"] = str(api_secret)
        await TradeControl.save_config(api_key, api_secret)
        return {"status": "ok"}

    if action == "manual_exit":
        symbol = data.get("symbol")
        if symbol:
            RAM_STATE["manual_exits"].add(symbol)
        return {"status": "ok"}

    if action == "square_off_one":
        side = data.get("side")
        symbol = data.get("symbol")
        if not side or not symbol:
            return {"status": "error", "message": "side/symbol missing"}

        stock = next((s for s in RAM_STATE["stocks"].values() if s.get("symbol") == symbol), None)
        if not stock:
            return {"status": "error", "message": "symbol not found"}

        try:
            if side in ("bull", "bear") and stock.get("brk_status") == "OPEN" and (stock.get("brk_side_latch") or "").lower() == side:
                await BreakoutEngine.close_position(stock, RAM_STATE, "UI_EXIT_ONE")
            elif side in ("mom_bull", "mom_bear") and stock.get("mom_status") == "OPEN" and (stock.get("mom_side_latch") or "").lower() == side:
                await MomentumEngine.close_position(stock, RAM_STATE, "UI_EXIT_ONE")
            else:
                return {"status": "ok", "message": "no open position for that side"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

        return {"status": "ok"}

    if action == "square_off_all":
        side = data.get("side")
        if side not in ("bull", "bear", "mom_bull", "mom_bear"):
            return {"status": "error", "message": "invalid side"}

        try:
            if side in ("bull", "bear"):
                for s in list(RAM_STATE["stocks"].values()):
                    if s.get("brk_status") == "OPEN" and (s.get("brk_side_latch") or "").lower() == side:
                        await BreakoutEngine.close_position(s, RAM_STATE, "UI_EXIT_ALL")
            else:
                for s in list(RAM_STATE["stocks"].values()):
                    if s.get("mom_status") == "OPEN" and (s.get("mom_side_latch") or "").lower() == side:
                        await MomentumEngine.close_position(s, RAM_STATE, "UI_EXIT_ALL")
        except Exception as e:
            return {"status": "error", "message": str(e)}

        return {"status": "ok"}

    return {"status": "error", "message": "unknown action"}

@app.post("/api/tick")
async def ingest_tick(data: Dict[str, Any]):
    token = int(data.get("token", 0) or 0)
    ltp = float(data.get("ltp", 0) or 0)
    vol = int(data.get("vol", 0) or 0)

    stock = RAM_STATE["stocks"].get(token)
    if not stock:
        return {"status": "error", "message": "token not found"}

    stock["ltp"] = ltp

    closed = _update_candle(stock, ltp, vol)
    if closed:
        asyncio.create_task(BreakoutEngine.on_candle_close(token, closed, RAM_STATE))
        asyncio.create_task(MomentumEngine.on_candle_close(token, closed, RAM_STATE))

    await BreakoutEngine.run(token, ltp, vol, RAM_STATE)
    await MomentumEngine.run(token, ltp, vol, RAM_STATE)

    RAM_STATE["data_connected"]["breakout"] = True
    RAM_STATE["data_connected"]["momentum"] = True
    return {"status": "ok"}
