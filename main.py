import os
import json
import time
import struct
import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pytz
import websockets
from websockets import WebSocketClientProtocol

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# Redis manager + control wrapper
from redis_manager import TradeControl, get_redis


# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MAIN] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

IST = pytz.timezone("Asia/Kolkata")


# -----------------------------
# Config
# -----------------------------
APP_DIR = os.path.dirname(os.path.abspath(__file__))
INDEX_HTML = os.getenv("INDEX_HTML_PATH", os.path.join(APP_DIR, "index.html"))

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")

# Dhan WS endpoint (v2)
DHAN_WS_BASE = os.getenv("DHAN_WS_BASE", "wss://api-feed.dhan.co")
DHAN_WS_AUTH_TYPE = os.getenv("DHAN_WS_AUTH_TYPE", "2")

# Subscribe mode:
# RequestCode 15 = Ticker, 17 = Quote, 21 = Full
DHAN_SUBSCRIBE_REQUEST_CODE = int(os.getenv("DHAN_SUBSCRIBE_REQUEST_CODE", "17"))

# Dhan per-message instrument limit
DHAN_SUBSCRIBE_CHUNK = int(os.getenv("DHAN_SUBSCRIBE_CHUNK", "100"))

# Optional small sleep between subscription batches (prevents burst)
SUBSCRIBE_BATCH_SLEEP = float(os.getenv("SUBSCRIBE_BATCH_SLEEP", "0.05"))

# Tick queues
TICK_QUEUE_MAX = int(os.getenv("TICK_QUEUE_MAX", "20000"))

# UI expected engine sides
ENGINE_SIDES = ["bull", "mom_bull", "bear", "mom_bear"]


# -----------------------------
# Binary parsing (Dhan WS is little-endian)
# Header: 8 bytes -> code(1), length(2), exch(1), secid(4)
# -----------------------------
HDR_STRUCT = struct.Struct("<BHBI")  # 1 + 2 + 1 + 4 = 8 bytes

# Ticker packet (code 2): ltp(float32), ltt(int32 epoch)
TICKER_STRUCT = struct.Struct("<fI")

# PrevClose packet (code 6): prev_close(float32), oi(int32)
PREVCLOSE_STRUCT = struct.Struct("<fI")

# Quote packet (code 4):
# ltp(f), ltq(h), ltt(I), atp(f), vol(I), tsq(I), tbq(I), open(f), close(f), high(f), low(f)
QUOTE_STRUCT = struct.Struct("<f h I f I I I f f f f")


def _parse_bool(v: Any) -> bool:
    """
    Robust bool parser.
    Fixes the classic bug: bool("0") == True.
    Accepts: true/false, "1"/"0", 1/0, "on"/"off", etc.
    """
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return int(v) != 0
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("1", "true", "yes", "on"):
            return True
        if s in ("0", "false", "no", "off", ""):
            return False
    return False


@dataclass(frozen=True)
class Tick:
    token: int
    symbol: str
    ltp: float
    ltt_epoch: int
    volume: Optional[int] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    prev_close: Optional[float] = None


# -----------------------------
# Global runtime state (in-memory for ultra-low latency)
# -----------------------------
class RuntimeState:
    def __init__(self) -> None:
        self.redis_ok: bool = False
        self.dhan_connected: bool = False
        self.dhan_last_msg_ts: float = 0.0

        self.universe_tokens: List[int] = []
        self.universe_symbols: List[str] = []
        self.token_to_symbol: Dict[int, str] = {}

        # engine toggle state (string "1"/"0" for UI)
        self.engine_status: Dict[str, str] = {s: "0" for s in ENGINE_SIDES}

        # pnl placeholder
        self.pnl: Dict[str, float] = {s: 0.0 for s in ENGINE_SIDES}
        self.pnl["total"] = 0.0

        # order book placeholder
        self.orders: Dict[str, List[Dict[str, Any]]] = {s: [] for s in ENGINE_SIDES}

        # scanner signals: side -> list[dict]
        self.scanner: Dict[str, List[Dict[str, Any]]] = {s: [] for s in ENGINE_SIDES}

        # cached daily levels from redis market_data (token->dict)
        self.market_data: Dict[int, Dict[str, Any]] = {}

        # queues/tasks
        self.stop_event = asyncio.Event()
        self.tick_queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=TICK_QUEUE_MAX)

        self.ws_task: Optional[asyncio.Task] = None
        self.dispatch_task: Optional[asyncio.Task] = None

        # For clean websocket shutdown
        self._ws: Optional[WebSocketClientProtocol] = None


STATE = RuntimeState()


# -----------------------------
# Redis helpers
# -----------------------------
async def _get_redis_client():
    """
    Always use the shared Redis client from redis_manager.get_redis()
    so TLS/Heroku config is consistent everywhere.
    """
    return await get_redis()


async def _load_universe_from_redis() -> Tuple[List[int], List[str]]:
    """
    Load universe tokens/symbols.
    Primary storage used by sync_market_data.py:
      nexus:universe:tokens
      nexus:universe:symbols
    """
    # 1) Try TradeControl helpers if present
    # (your TradeControl includes get_subscribe_universe_tokens)
    try:
        fn_tokens = getattr(TradeControl, "get_subscribe_universe_tokens", None)
        if callable(fn_tokens):
            tokens = await fn_tokens()
            tokens = [int(x) for x in (tokens or [])]
            # symbols are stored in Redis; read directly
            r = await _get_redis_client()
            syms_raw = await r.get("nexus:universe:symbols")
            symbols: List[str] = []
            if syms_raw:
                try:
                    symbols = [str(x) for x in json.loads(syms_raw)]
                except Exception:
                    symbols = [x.strip().upper() for x in str(syms_raw).split(",") if x.strip()]
            return tokens, symbols
    except Exception:
        pass

    # 2) Fallback: direct Redis keys
    r = await _get_redis_client()
    tokens_raw = await r.get("nexus:universe:tokens")
    syms_raw = await r.get("nexus:universe:symbols")

    tokens: List[int] = []
    symbols: List[str] = []

    if tokens_raw:
        try:
            tokens = [int(x) for x in json.loads(tokens_raw)]
        except Exception:
            tokens = [int(x) for x in str(tokens_raw).split(",") if x.strip().isdigit()]

    if syms_raw:
        try:
            symbols = [str(x) for x in json.loads(syms_raw)]
        except Exception:
            symbols = [x.strip().upper() for x in str(syms_raw).split(",") if x.strip()]

    return tokens, symbols


async def _load_market_data_bulk(tokens: List[int]) -> Dict[int, Dict[str, Any]]:
    """
    Load cached market data (pdh/pdl/prev_close/etc) into memory for fast scanner.
    Uses TradeControl.get_market_data(token) primarily.
    Fallback reads the correct key used by redis_manager.py: nexus:market:{token}
    """
    out: Dict[int, Dict[str, Any]] = {}
    r = await _get_redis_client()

    get_md = getattr(TradeControl, "get_market_data", None)

    for t in tokens:
        try:
            if callable(get_md):
                md = await get_md(str(t))
                if md:
                    out[int(t)] = md
                    continue

            # FIX: correct key is nexus:market:{token} (not nexus:market_data:{token})
            raw = await r.get(f"nexus:market:{t}")
            if raw:
                out[int(t)] = json.loads(raw)
        except Exception:
            continue

    return out


async def _load_engine_status() -> None:
    """
    Load engine toggles from Redis (kept separate from TradeControl).
    """
    r = await _get_redis_client()
    for side in ENGINE_SIDES:
        v = await r.get(f"nexus:engine:{side}:enabled")
        if v is not None:
            STATE.engine_status[side] = "1" if str(v).strip().lower() in ("1", "true", "yes", "on") else "0"


async def _save_engine_status(side: str, enabled: bool) -> None:
    r = await _get_redis_client()
    await r.set(f"nexus:engine:{side}:enabled", "1" if enabled else "0")
    STATE.engine_status[side] = "1" if enabled else "0"


# -----------------------------
# Dhan WebSocket worker
# -----------------------------
def _build_dhan_ws_url(access_token: str, client_id: str) -> str:
    return (
        f"{DHAN_WS_BASE}"
        f"?version=2"
        f"&token={access_token}"
        f"&clientId={client_id}"
        f"&authType={DHAN_WS_AUTH_TYPE}"
    )


def _chunk_instruments(tokens: List[int]) -> List[List[int]]:
    return [tokens[i: i + DHAN_SUBSCRIBE_CHUNK] for i in range(0, len(tokens), DHAN_SUBSCRIBE_CHUNK)]


async def _send_subscriptions(ws: WebSocketClientProtocol, tokens: List[int]) -> None:
    """
    Send multiple JSON messages (100 instruments max per message).
    """
    batches = _chunk_instruments(tokens)
    for batch in batches:
        payload = {
            "RequestCode": DHAN_SUBSCRIBE_REQUEST_CODE,
            "InstrumentCount": len(batch),
            "InstrumentList": [{"ExchangeSegment": "NSE_EQ", "SecurityId": str(t)} for t in batch],
        }
        await ws.send(json.dumps(payload))
        if SUBSCRIBE_BATCH_SLEEP > 0:
            await asyncio.sleep(SUBSCRIBE_BATCH_SLEEP)


def _parse_binary_message(data: bytes) -> Optional[Tick]:
    """
    Parse Dhan binary message into Tick.
    Handles:
      code 2: ticker
      code 4: quote
      code 6: prev close
      code 50: disconnect
    """
    if not data or len(data) < 8:
        return None

    try:
        feed_code, msg_len, exch, sec_id = HDR_STRUCT.unpack_from(data, 0)
    except Exception:
        return None

    token = int(sec_id)
    symbol = STATE.token_to_symbol.get(token, str(token))

    # code 2 = ticker packet
    if feed_code == 2 and len(data) >= 8 + TICKER_STRUCT.size:
        ltp, ltt = TICKER_STRUCT.unpack_from(data, 8)
        return Tick(token=token, symbol=symbol, ltp=float(ltp), ltt_epoch=int(ltt))

    # code 4 = quote packet
    if feed_code == 4 and len(data) >= 8 + QUOTE_STRUCT.size:
        (
            ltp, ltq, ltt, atp, vol, tsq, tbq,
            day_open, day_close, day_high, day_low
        ) = QUOTE_STRUCT.unpack_from(data, 8)
        return Tick(
            token=token,
            symbol=symbol,
            ltp=float(ltp),
            ltt_epoch=int(ltt),
            volume=int(vol),
            open=float(day_open),
            high=float(day_high),
            low=float(day_low),
        )

    # code 6 = prev close
    if feed_code == 6 and len(data) >= 8 + PREVCLOSE_STRUCT.size:
        prev_close, oi = PREVCLOSE_STRUCT.unpack_from(data, 8)
        md = STATE.market_data.get(token) or {}
        md["prev_close"] = float(prev_close)
        STATE.market_data[token] = md
        return None

    # code 50 = disconnect packet (server side)
    if feed_code == 50:
        STATE.dhan_connected = False
        return None

    return None


async def dhan_marketfeed_worker() -> None:
    """
    Keeps websocket alive, reconnects, subscribes in chunks, pushes ticks to queue.
    """
    backoff = 1.0
    max_backoff = 20.0

    while not STATE.stop_event.is_set():
        try:
            # creds from redis
            client_id, _ = await TradeControl.get_config()
            access_token = await TradeControl.get_access_token()

            if not client_id or not access_token:
                STATE.dhan_connected = False
                logger.error("Dhan creds missing in Redis (client_id/access_token).")
                await asyncio.sleep(2.0)
                continue

            url = _build_dhan_ws_url(access_token, client_id)
            logger.info("Marketfeed worker connecting...")

            async with websockets.connect(
                url,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=2,
                max_queue=None,
                compression=None,
            ) as ws:
                STATE._ws = ws
                STATE.dhan_connected = True
                STATE.dhan_last_msg_ts = time.time()
                backoff = 1.0

                # subscribe universe
                if STATE.universe_tokens:
                    await _send_subscriptions(ws, STATE.universe_tokens)
                    logger.info(f"Subscribed tokens={len(STATE.universe_tokens)} in batches of {DHAN_SUBSCRIBE_CHUNK}.")
                else:
                    logger.warning("Universe is empty; no subscriptions sent.")

                # receive loop
                while not STATE.stop_event.is_set():
                    msg = await ws.recv()
                    STATE.dhan_last_msg_ts = time.time()

                    if isinstance(msg, bytes):
                        tick = _parse_binary_message(msg)
                        if tick:
                            try:
                                STATE.tick_queue.put_nowait(tick)
                            except asyncio.QueueFull:
                                pass
                    # ignore text frames

        except asyncio.CancelledError:
            break
        except Exception as e:
            STATE.dhan_connected = False
            STATE._ws = None
            logger.error(f"Marketfeed error: {e}")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 1.6)

    # graceful shutdown
    try:
        if STATE._ws is not None:
            await STATE._ws.send(json.dumps({"RequestCode": 12}))
            await STATE._ws.close()
    except Exception:
        pass
    finally:
        STATE._ws = None
        STATE.dhan_connected = False
        logger.info("Marketfeed worker exited cleanly.")


# -----------------------------
# Ultra-low latency dispatcher + basic scanner
# -----------------------------
def _push_scanner_signal(side: str, symbol: str, trigger_px: float, reason: str) -> None:
    lst = STATE.scanner.get(side) or []
    lst.insert(
        0,
        {
            "symbol": symbol,
            "trigger_px": float(trigger_px),
            "seen_time": time.strftime("%H:%M:%S", time.localtime()),
            "reason": reason,
        },
    )
    STATE.scanner[side] = lst[:200]


async def tick_dispatcher() -> None:
    """
    Reads ticks from queue; updates simple scanner logic.
    Hook your real engines here if needed.
    """
    logger.info("Parallel tick worker active.")
    while not STATE.stop_event.is_set():
        try:
            tick = await STATE.tick_queue.get()
        except asyncio.CancelledError:
            break

        # Simple scanner example using PDH/PDL from market_data
        md = STATE.market_data.get(tick.token)
        if md:
            pdh = float(md.get("pdh") or 0.0)
            pdl = float(md.get("pdl") or 0.0)

            if pdh > 0 and tick.ltp >= pdh:
                _push_scanner_signal("bull", tick.symbol, tick.ltp, "LTP >= PDH")

            if pdl > 0 and tick.ltp <= pdl:
                _push_scanner_signal("bear", tick.symbol, tick.ltp, "LTP <= PDL")


# -----------------------------
# FastAPI app + routes
# -----------------------------
app = FastAPI(title="Nexus Core", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    if not os.path.exists(INDEX_HTML):
        return JSONResponse({"error": "index.html not found", "path": INDEX_HTML}, status_code=404)
    return FileResponse(INDEX_HTML)


@app.get("/api/stats")
async def api_stats():
    data_connected = {
        "breakout": STATE.engine_status.get("bull") == "1" or STATE.engine_status.get("bear") == "1",
        "momentum": STATE.engine_status.get("mom_bull") == "1" or STATE.engine_status.get("mom_bear") == "1",
        "dhan": STATE.dhan_connected,
    }
    return {
        "redis_ok": STATE.redis_ok,
        "dhan_connected": STATE.dhan_connected,
        "dhan_last_msg_sec_ago": round(time.time() - (STATE.dhan_last_msg_ts or time.time()), 2),
        "engine_status": STATE.engine_status,
        "data_connected": data_connected,
        "pnl": STATE.pnl,
    }


@app.get("/api/orders")
async def api_orders():
    return STATE.orders


@app.get("/api/scanner")
async def api_scanner():
    return STATE.scanner


@app.get("/api/config/auth")
async def api_config_auth():
    client_id, _ = await TradeControl.get_config()
    access_token = await TradeControl.get_access_token()
    return {"client_id": client_id or "", "access_token": access_token or ""}


@app.post("/api/settings/engine/{side}")
async def save_engine_settings(side: str, payload: Dict[str, Any]):
    if side not in ENGINE_SIDES:
        raise HTTPException(status_code=400, detail="Invalid side")

    # Prefer TradeControl if your engines use it (your redis_manager uses save_strategy_settings)
    fn = getattr(TradeControl, "save_strategy_settings", None)
    if callable(fn):
        ok = await fn(side, payload)
        if not ok:
            raise HTTPException(status_code=500, detail="Failed to save settings")
        return {"status": "success"}

    # fallback direct redis
    r = await _get_redis_client()
    await r.set(f"nexus:settings:{side}", json.dumps(payload))
    return {"status": "success"}


@app.get("/api/settings/engine/{side}")
async def get_engine_settings(side: str):
    if side not in ENGINE_SIDES:
        raise HTTPException(status_code=400, detail="Invalid side")

    fn = getattr(TradeControl, "get_strategy_settings", None)
    if callable(fn):
        data = await fn(side)
        return data or {}

    r = await _get_redis_client()
    raw = await r.get(f"nexus:settings:{side}")
    return json.loads(raw) if raw else {}


@app.post("/api/control")
async def api_control(payload: Dict[str, Any]):
    action = payload.get("action")

    if action == "toggle_engine":
        side = payload.get("side")
        enabled = _parse_bool(payload.get("enabled", payload.get("enabled_str", payload.get("enabled_int"))))
        if side not in ENGINE_SIDES:
            raise HTTPException(status_code=400, detail="Invalid side")
        await _save_engine_status(side, enabled)
        return {"status": "success", "engine_status": STATE.engine_status}

    if action == "save_api":
        # from UI: broker=dhan, client_id, access_token
        client_id = (payload.get("client_id") or "").strip()
        access_token = (payload.get("access_token") or "").strip()

        if not client_id or not access_token:
            raise HTTPException(status_code=400, detail="client_id and access_token required")

        save_fn = getattr(TradeControl, "save_config", None)
        if callable(save_fn):
            ok = await save_fn(client_id, access_token)
            if not ok:
                raise HTTPException(status_code=500, detail="Failed to save credentials")
        else:
            # fallback direct redis keys (kept for safety)
            r = await _get_redis_client()
            await r.set("nexus:config:api_key", client_id)
            await r.set("nexus:config:api_secret", access_token)
            await r.set("nexus:auth:access_token", access_token)

        return {"status": "success"}

    if action == "square_off_one":
        side = payload.get("side")
        symbol = payload.get("symbol")
        if side in STATE.orders:
            STATE.orders[side] = [o for o in STATE.orders[side] if o.get("symbol") != symbol]
        return {"status": "success"}

    if action == "square_off_all":
        side = payload.get("side")
        if side in STATE.orders:
            STATE.orders[side] = []
        return {"status": "success"}

    raise HTTPException(status_code=400, detail="Unknown action")


# -----------------------------
# Startup / shutdown
# -----------------------------
@app.on_event("startup")
async def on_startup():
    logger.info("System startup")

    # Redis init
    try:
        r = await _get_redis_client()
        await r.ping()
        STATE.redis_ok = True
        logger.info("Redis OK")
    except Exception as e:
        STATE.redis_ok = False
        logger.error(f"Redis init failed: {e}")

    # load engine toggles
    try:
        await _load_engine_status()
    except Exception:
        pass

    # load universe
    try:
        tokens, symbols = await _load_universe_from_redis()
        STATE.universe_tokens = tokens
        STATE.universe_symbols = symbols
        STATE.token_to_symbol = {int(t): symbols[i] for i, t in enumerate(tokens) if i < len(symbols)}
        logger.info(f"Universe loaded. tokens={len(tokens)}")
    except Exception as e:
        logger.error(f"Universe load failed: {e}")

    # load market_data cache (pdh/pdl/prev_close etc.)
    try:
        if STATE.universe_tokens:
            STATE.market_data = await _load_market_data_bulk(STATE.universe_tokens)
            logger.info(f"Market data cached. tokens={len(STATE.market_data)}")
    except Exception:
        pass

    # start tasks
    STATE.stop_event.clear()
    STATE.dispatch_task = asyncio.create_task(tick_dispatcher(), name="tick_dispatcher")
    STATE.ws_task = asyncio.create_task(dhan_marketfeed_worker(), name="dhan_marketfeed_worker")


@app.on_event("shutdown")
async def on_shutdown():
    logger.info("Shutdown requested")
    STATE.stop_event.set()

    # cancel tasks
    for t in (STATE.ws_task, STATE.dispatch_task):
        if t and not t.done():
            t.cancel()

    # wait tasks
    for t in (STATE.ws_task, STATE.dispatch_task):
        if t:
            try:
                await t
            except Exception:
                pass

    # best-effort close websocket
    try:
        if STATE._ws is not None:
            await STATE._ws.send(json.dumps({"RequestCode": 12}))
            await STATE._ws.close()
    except Exception:
        pass

    logger.info("Shutdown complete")
