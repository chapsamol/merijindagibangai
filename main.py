# main.py (FULL FIXED VERSION)
# - Adds BreakoutEngine wiring (ticks -> BreakoutEngine.run)
# - Adds 1-min candle builder (ticks -> candle close -> BreakoutEngine.on_candle_close)
# - Adds Dhan Order Update websocket worker + status for dashboard
# - Keeps your existing marketfeed worker + FastAPI routes
#
# IMPORTANT (Heroku / production):
#   Run SINGLE worker, otherwise multiple processes will open multiple websockets:
#     web: uvicorn main:app --host 0.0.0.0 --port $PORT --workers 1

import os
import json
import time
import struct
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pytz
import websockets
from websockets import WebSocketClientProtocol

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# Redis manager + control wrapper
from redis_manager import TradeControl, get_redis

# Your engine
from breakout_engine import BreakoutEngine

# Optional Dhan SDK (your BreakoutEngine expects .BUY/.SELL/.NSE/.MARKET/.INTRA and .place_order)
try:
    from dhanhq import DhanContext, dhanhq  # type: ignore
except Exception:
    DhanContext = None
    dhanhq = None


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

# Dhan Marketfeed WS endpoint (v2)
DHAN_WS_BASE = os.getenv("DHAN_WS_BASE", "wss://api-feed.dhan.co")
DHAN_WS_AUTH_TYPE = os.getenv("DHAN_WS_AUTH_TYPE", "2")

# Dhan Order Update WS endpoint (separate)
DHAN_ORDER_WS = os.getenv("DHAN_ORDER_WS", "wss://api-order-update.dhan.co")

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

# Candle timeframe (minutes) - BreakoutEngine expects candle closes
CANDLE_TF_MINUTES = int(os.getenv("CANDLE_TF_MINUTES", "1"))

# Websocket keepalive
WS_PING_INTERVAL = float(os.getenv("WS_PING_INTERVAL", "20"))
WS_PING_TIMEOUT = float(os.getenv("WS_PING_TIMEOUT", "20"))


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
    """Robust bool parser. Fixes bool('0') == True bug."""
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
    volume: Optional[int] = None  # day volume (if available)
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

        # Marketfeed WS status
        self.dhan_connected: bool = False
        self.dhan_last_msg_ts: float = 0.0

        # Order-update WS status
        self.order_updates_connected: bool = False
        self.order_last_msg_ts: float = 0.0

        self.universe_tokens: List[int] = []
        self.universe_symbols: List[str] = []
        self.token_to_symbol: Dict[int, str] = {}

        # engine toggle state (string "1"/"0" for UI)
        self.engine_status: Dict[str, str] = {s: "0" for s in ENGINE_SIDES}

        # pnl placeholder
        self.pnl: Dict[str, float] = {s: 0.0 for s in ENGINE_SIDES}
        self.pnl["total"] = 0.0

        # order book placeholder (for UI)
        self.orders: Dict[str, List[Dict[str, Any]]] = {s: [] for s in ENGINE_SIDES}

        # scanner signals: side -> list[dict]
        self.scanner: Dict[str, List[Dict[str, Any]]] = {s: [] for s in ENGINE_SIDES}

        # cached daily levels from redis market_data (token->dict)
        self.market_data: Dict[int, Dict[str, Any]] = {}

        # queues/tasks
        self.stop_event = asyncio.Event()
        self.tick_queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=TICK_QUEUE_MAX)

        self.ws_task: Optional[asyncio.Task] = None
        self.order_ws_task: Optional[asyncio.Task] = None
        self.dispatch_task: Optional[asyncio.Task] = None

        # For clean websocket shutdown
        self._ws: Optional[WebSocketClientProtocol] = None
        self._order_ws: Optional[WebSocketClientProtocol] = None


STATE = RuntimeState()


# -----------------------------
# Engine runtime state (shared with BreakoutEngine)
# -----------------------------
ENGINE_STATE: Dict[str, Any] = {
    "stocks": {},  # token -> stock dict
    "config": {},  # side -> config dict
    "engine_live": {},  # side -> bool (MUST be bool)
    "trades": {"bull": [], "bear": []},  # lists of trades
    "manual_exits": set(),  # symbols
    "dhan": None,  # Dhan client
}

# Per-token locks (BreakoutEngine comment expects per-symbol lock handled outside)
TOKEN_LOCKS: Dict[int, asyncio.Lock] = {}


# -----------------------------
# Redis helpers
# -----------------------------
async def _get_redis_client():
    """Always use the shared Redis client from redis_manager.get_redis()."""
    return await get_redis()


async def _load_universe_from_redis() -> Tuple[List[int], List[str]]:
    """
    Load universe tokens/symbols.
    Primary storage:
      nexus:universe:tokens
      nexus:universe:symbols
    """
    try:
        fn_tokens = getattr(TradeControl, "get_subscribe_universe_tokens", None)
        if callable(fn_tokens):
            tokens = await fn_tokens()
            tokens = [int(x) for x in (tokens or [])]
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
    Load cached market data (pdh/pdl/prev_close/etc) into memory for fast scanner/engine.
    Primary: TradeControl.get_market_data(token)
    Fallback: redis key nexus:market:{token}
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

            raw = await r.get(f"nexus:market:{t}")
            if raw:
                out[int(t)] = json.loads(raw)
        except Exception:
            continue

    return out


async def _load_engine_status() -> None:
    """Load engine toggles from Redis."""
    r = await _get_redis_client()
    for side in ENGINE_SIDES:
        v = await r.get(f"nexus:engine:{side}:enabled")
        if v is not None:
            STATE.engine_status[side] = "1" if str(v).strip().lower() in ("1", "true", "yes", "on") else "0"


async def _save_engine_status(side: str, enabled: bool) -> None:
    r = await _get_redis_client()
    await r.set(f"nexus:engine:{side}:enabled", "1" if enabled else "0")
    STATE.engine_status[side] = "1" if enabled else "0"

    # Keep ENGINE_STATE in sync (BreakoutEngine needs bools)
    if side in ("bull", "bear"):
        ENGINE_STATE["engine_live"][side] = bool(enabled)


async def _load_engine_settings(side: str) -> Dict[str, Any]:
    """Load per-engine settings. Prefers TradeControl.get_strategy_settings."""
    fn = getattr(TradeControl, "get_strategy_settings", None)
    if callable(fn):
        data = await fn(side)
        return data or {}

    r = await _get_redis_client()
    raw = await r.get(f"nexus:settings:{side}")
    return json.loads(raw) if raw else {}


async def _save_engine_settings(side: str, payload: Dict[str, Any]) -> bool:
    """Save per-engine settings. Prefers TradeControl.save_strategy_settings."""
    fn = getattr(TradeControl, "save_strategy_settings", None)
    if callable(fn):
        ok = await fn(side, payload)
        return bool(ok)

    r = await _get_redis_client()
    await r.set(f"nexus:settings:{side}", json.dumps(payload))
    return True


# -----------------------------
# Dhan Marketfeed WS helpers
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
    """Send multiple JSON subscription messages (<=100 instruments per message)."""
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
    """Keeps marketfeed websocket alive, reconnects, subscribes, pushes ticks to queue."""
    backoff = 1.0
    max_backoff = 20.0

    while not STATE.stop_event.is_set():
        try:
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
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                close_timeout=2,
                max_queue=None,
                compression=None,
            ) as ws:
                STATE._ws = ws
                STATE.dhan_connected = True
                STATE.dhan_last_msg_ts = time.time()
                backoff = 1.0

                if STATE.universe_tokens:
                    await _send_subscriptions(ws, STATE.universe_tokens)
                    logger.info(f"Subscribed tokens={len(STATE.universe_tokens)} in batches of {DHAN_SUBSCRIBE_CHUNK}.")
                else:
                    logger.warning("Universe is empty; no subscriptions sent.")

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
# Dhan Order Update WS worker
# -----------------------------
def _safe_get(d: Any, *keys: str) -> Optional[Any]:
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return None


def _extract_order_id_any(payload: Any) -> Optional[str]:
    if payload is None:
        return None
    if isinstance(payload, dict):
        for k in ("orderId", "order_id", "OrderId", "orderNo", "OrderNo"):
            v = payload.get(k)
            if v:
                return str(v)
        # nested
        for k in ("data", "Data", "order", "Order", "result", "Result"):
            v = payload.get(k)
            oid = _extract_order_id_any(v)
            if oid:
                return oid
    return None


def _extract_avg_price(payload: Any) -> Optional[float]:
    if not isinstance(payload, dict):
        return None
    # try common keys
    for k in (
        "averageTradedPrice", "avgTradedPrice", "AvgTradedPrice",
        "average_price", "avg_price", "tradedPrice", "TradedPrice"
    ):
        v = payload.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass
    # nested
    for k in ("data", "Data", "order", "Order", "result", "Result"):
        v = payload.get(k)
        ap = _extract_avg_price(v) if isinstance(v, dict) else None
        if ap is not None:
            return ap
    return None


def _extract_status(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    for k in ("orderStatus", "status", "Status", "order_status"):
        v = payload.get(k)
        if v:
            return str(v).upper()
    for k in ("data", "Data", "order", "Order", "result", "Result"):
        v = payload.get(k)
        st = _extract_status(v) if isinstance(v, dict) else None
        if st:
            return st
    return None


def _is_filled_status(st: Optional[str]) -> bool:
    if not st:
        return False
    s = st.upper()
    return any(x in s for x in ("TRADED", "COMPLETE", "FILLED", "EXECUTED"))


def _is_rejected_status(st: Optional[str]) -> bool:
    if not st:
        return False
    s = st.upper()
    return any(x in s for x in ("REJECT", "CANCEL"))


def _recompute_trade_from_exec(trade: Dict[str, Any], exec_price: float) -> None:
    """Recompute target/trail based on executed entry price (your engine comment expects this in main.py)."""
    try:
        exec_price = float(exec_price)
        if exec_price <= 0:
            return
        trade["entry_exec_price"] = exec_price
        trade["entry_price"] = exec_price

        sl = float(trade.get("sl_price") or 0.0)
        rr_val = float(trade.get("rr_val") or 2.0)
        tsl_ratio = float(trade.get("tsl_ratio") or 1.5)

        init_risk = abs(exec_price - sl)
        if init_risk <= 0:
            return

        trade["init_risk"] = float(init_risk)

        side = str(trade.get("side") or "").lower()
        if side == "bull":
            trade["target_price"] = round(exec_price + (init_risk * rr_val), 2)
        elif side == "bear":
            trade["target_price"] = round(exec_price - (init_risk * rr_val), 2)

        trade["trail_step"] = float(init_risk * tsl_ratio) if tsl_ratio > 0 else float(init_risk)
    except Exception:
        return


def _sync_ui_orders_from_trades() -> None:
    """Keep STATE.orders updated for UI."""
    try:
        STATE.orders["bull"] = list(ENGINE_STATE["trades"].get("bull", []))
        STATE.orders["bear"] = list(ENGINE_STATE["trades"].get("bear", []))
    except Exception:
        pass


async def dhan_order_updates_worker() -> None:
    """Connects to Dhan order update WS and updates trade execution fields for the engine."""
    backoff = 1.0
    max_backoff = 20.0

    while not STATE.stop_event.is_set():
        try:
            client_id, _ = await TradeControl.get_config()
            access_token = await TradeControl.get_access_token()

            if not client_id or not access_token:
                STATE.order_updates_connected = False
                await asyncio.sleep(2.0)
                continue

            logger.info("OrderUpdate worker connecting...")

            async with websockets.connect(
                DHAN_ORDER_WS,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
                close_timeout=2,
                max_queue=None,
                compression=None,
            ) as ws:
                STATE._order_ws = ws
                STATE.order_updates_connected = True
                STATE.order_last_msg_ts = time.time()
                backoff = 1.0

                # Mandatory login/auth for order update websocket
                login = {
                    "LoginReq": {"MsgCode": 42, "ClientId": str(client_id), "Token": str(access_token)},
                    "UserType": "SELF",
                }
                await ws.send(json.dumps(login))

                while not STATE.stop_event.is_set():
                    msg = await ws.recv()
                    STATE.order_last_msg_ts = time.time()

                    if not isinstance(msg, str):
                        continue

                    try:
                        payload = json.loads(msg)
                    except Exception:
                        continue

                    oid = _extract_order_id_any(payload)
                    st = _extract_status(payload)
                    avgp = _extract_avg_price(payload)

                    # Update trades by matching order id
                    if oid:
                        for side in ("bull", "bear"):
                            for trade in ENGINE_STATE["trades"].get(side, []):
                                # entry
                                if str(trade.get("order_id")) == str(oid):
                                    if avgp and _is_filled_status(st):
                                        # executed entry
                                        _recompute_trade_from_exec(trade, float(avgp))
                                    if _is_rejected_status(st):
                                        trade["status"] = "REJECTED"
                                        trade["reject_reason"] = _safe_get(payload, "reason", "Reason", "message", "Message") or str(st)
                                    break

                                # exit
                                if trade.get("exit_order_id") and str(trade.get("exit_order_id")) == str(oid):
                                    if avgp and _is_filled_status(st):
                                        trade["exit_exec_price"] = float(avgp)
                                        trade["status"] = "CLOSED"
                                        trade["exit_time"] = datetime.now(IST).strftime("%H:%M:%S")
                                    if _is_rejected_status(st):
                                        trade["exit_reject_reason"] = _safe_get(payload, "reason", "Reason", "message", "Message") or str(st)
                                    break

                    # push raw last few updates into UI orders if you want (optional)
                    _sync_ui_orders_from_trades()

        except asyncio.CancelledError:
            break
        except Exception as e:
            STATE.order_updates_connected = False
            STATE._order_ws = None
            logger.error(f"OrderUpdate WS error: {e}")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 1.6)

    # shutdown
    STATE.order_updates_connected = False
    STATE._order_ws = None
    logger.info("OrderUpdate worker exited cleanly.")


# -----------------------------
# Ultra-low latency dispatcher + scanner + candle builder + engine runner
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


def _bucket_epoch_minute(epoch: int, tf_min: int) -> int:
    """Return bucket start epoch (seconds) for tf minutes."""
    if epoch <= 0:
        return 0
    tf_sec = int(tf_min) * 60
    return int(epoch // tf_sec) * tf_sec


async def tick_dispatcher() -> None:
    """
    Reads ticks from queue:
    - updates scanner (pdh/pdl)
    - builds candles and calls BreakoutEngine.on_candle_close()
    - calls BreakoutEngine.run() on each tick (tick-fast)
    """
    logger.info("Tick dispatcher active (scanner + candles + breakout engine).")

    # candle state: token -> current candle dict + bucket
    cur_bucket: Dict[int, int] = {}
    cur_candle: Dict[int, Dict[str, Any]] = {}

    # to compute per-minute volume from day volume (if available)
    last_day_vol: Dict[int, int] = {}

    while not STATE.stop_event.is_set():
        try:
            tick = await STATE.tick_queue.get()
        except asyncio.CancelledError:
            break

        # Update LTP into engine stock (if exists)
        stock = ENGINE_STATE["stocks"].get(tick.token)
        if stock:
            stock["ltp"] = float(tick.ltp)

        # Simple scanner using PDH/PDL from cached market_data
        md = STATE.market_data.get(tick.token)
        if md:
            pdh = float(md.get("pdh") or 0.0)
            pdl = float(md.get("pdl") or 0.0)

            if pdh > 0 and tick.ltp >= pdh:
                _push_scanner_signal("bull", tick.symbol, tick.ltp, "LTP >= PDH")

            if pdl > 0 and tick.ltp <= pdl:
                _push_scanner_signal("bear", tick.symbol, tick.ltp, "LTP <= PDL")

        # Candle builder (1m by default)
        bucket = _bucket_epoch_minute(int(tick.ltt_epoch or 0), CANDLE_TF_MINUTES)
        if bucket > 0:
            prev_bucket = cur_bucket.get(tick.token)

            # compute per-minute volume delta (best effort)
            v_delta = 0
            if tick.volume is not None:
                prev_v = last_day_vol.get(tick.token)
                if prev_v is None:
                    v_delta = 0
                else:
                    v_delta = max(0, int(tick.volume) - int(prev_v))
                last_day_vol[tick.token] = int(tick.volume)

            if prev_bucket is None:
                cur_bucket[tick.token] = bucket
                cur_candle[tick.token] = {
                    "open": float(tick.ltp),
                    "high": float(tick.ltp),
                    "low": float(tick.ltp),
                    "close": float(tick.ltp),
                    "volume": int(v_delta),
                    "bucket_epoch": bucket,
                }
            elif bucket == prev_bucket:
                c = cur_candle.get(tick.token)
                if c:
                    c["high"] = max(float(c["high"]), float(tick.ltp))
                    c["low"] = min(float(c["low"]), float(tick.ltp))
                    c["close"] = float(tick.ltp)
                    c["volume"] = int(c.get("volume", 0)) + int(v_delta)
            else:
                # candle close: finalize previous
                closed = cur_candle.get(tick.token)
                if closed:
                    # call candle-close qualification
                    lock = TOKEN_LOCKS.get(tick.token)
                    if lock is None:
                        lock = asyncio.Lock()
                        TOKEN_LOCKS[tick.token] = lock

                    async with lock:
                        try:
                            await BreakoutEngine.on_candle_close(tick.token, dict(closed), ENGINE_STATE)
                        except Exception as e:
                            logger.error(f"CandleClose error token={tick.token}: {e}")

                # start new candle
                cur_bucket[tick.token] = bucket
                cur_candle[tick.token] = {
                    "open": float(tick.ltp),
                    "high": float(tick.ltp),
                    "low": float(tick.ltp),
                    "close": float(tick.ltp),
                    "volume": int(v_delta),
                    "bucket_epoch": bucket,
                }

        # Tick-fast engine
        lock = TOKEN_LOCKS.get(tick.token)
        if lock is None:
            lock = asyncio.Lock()
            TOKEN_LOCKS[tick.token] = lock

        async with lock:
            try:
                await BreakoutEngine.run(
                    token=int(tick.token),
                    ltp=float(tick.ltp),
                    vol=int(tick.volume or 0),
                    state=ENGINE_STATE,
                )
            except Exception as e:
                logger.error(f"Breakout run error token={tick.token}: {e}")

        # Keep UI orders view updated
        _sync_ui_orders_from_trades()


# -----------------------------
# FastAPI app + routes
# -----------------------------
app = FastAPI(title="Nexus Core", version="2.1")

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
        "breakout": (STATE.engine_status.get("bull") == "1" or STATE.engine_status.get("bear") == "1"),
        "momentum": (STATE.engine_status.get("mom_bull") == "1" or STATE.engine_status.get("mom_bear") == "1"),
        "dhan": STATE.dhan_connected,
        "order_updates": STATE.order_updates_connected,
    }
    return {
        "redis_ok": STATE.redis_ok,
        "dhan_connected": STATE.dhan_connected,
        "dhan_last_msg_sec_ago": round(time.time() - (STATE.dhan_last_msg_ts or time.time()), 2),
        "order_updates_connected": STATE.order_updates_connected,
        "order_last_msg_sec_ago": round(time.time() - (STATE.order_last_msg_ts or time.time()), 2),
        "engine_status": STATE.engine_status,
        "data_connected": data_connected,
        "pnl": STATE.pnl,
    }


@app.get("/api/orders")
async def api_orders():
    # always sync from engine trades
    _sync_ui_orders_from_trades()
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

    ok = await _save_engine_settings(side, payload)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to save settings")

    # Keep ENGINE_STATE config updated for breakout sides
    if side in ("bull", "bear"):
        ENGINE_STATE["config"][side] = payload or {}

    return {"status": "success"}


@app.get("/api/settings/engine/{side}")
async def get_engine_settings(side: str):
    if side not in ENGINE_SIDES:
        raise HTTPException(status_code=400, detail="Invalid side")
    data = await _load_engine_settings(side)
    return data or {}


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
            r = await _get_redis_client()
            await r.set("nexus:config:api_key", client_id)
            await r.set("nexus:config:api_secret", access_token)
            await r.set("nexus:auth:access_token", access_token)

        # refresh dhan client in ENGINE_STATE
        await _init_dhan_client_into_engine_state()

        return {"status": "success"}

    if action == "manual_exit_one":
        symbol = (payload.get("symbol") or "").strip().upper()
        if symbol:
            ENGINE_STATE["manual_exits"].add(symbol)
        return {"status": "success"}

    if action == "square_off_one":
        # UI-only cleanup (engine exits should use manual_exit_one)
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
# Startup / shutdown helpers
# -----------------------------
async def _init_engine_state_stocks() -> None:
    """Build ENGINE_STATE['stocks'] from universe + cached market data."""
    stocks: Dict[int, Dict[str, Any]] = {}

    for t in STATE.universe_tokens:
        md = STATE.market_data.get(int(t), {}) or {}
        sym = STATE.token_to_symbol.get(int(t), str(t))

        stocks[int(t)] = {
            "token": int(t),
            "security_id": int(t),  # your system uses token==security_id
            "symbol": sym,

            # levels + indicators
            "pdh": float(md.get("pdh") or 0.0),
            "pdl": float(md.get("pdl") or 0.0),
            "sma": float(md.get("sma") or 0.0),

            # engine fields
            "ltp": 0.0,
            "brk_status": "WAITING",
        }

    ENGINE_STATE["stocks"] = stocks

    # locks
    for t in STATE.universe_tokens:
        TOKEN_LOCKS.setdefault(int(t), asyncio.Lock())


async def _init_engine_configs() -> None:
    """Load configs into ENGINE_STATE['config'] (at least bull/bear)."""
    ENGINE_STATE["config"]["bull"] = await _load_engine_settings("bull")
    ENGINE_STATE["config"]["bear"] = await _load_engine_settings("bear")


async def _init_engine_live_flags() -> None:
    """Sync engine_live flags (bool) from STATE.engine_status (string)."""
    ENGINE_STATE["engine_live"]["bull"] = (STATE.engine_status.get("bull") == "1")
    ENGINE_STATE["engine_live"]["bear"] = (STATE.engine_status.get("bear") == "1")


async def _init_dhan_client_into_engine_state() -> None:
    """Create Dhan client and store in ENGINE_STATE['dhan']."""
    client_id, _ = await TradeControl.get_config()
    access_token = await TradeControl.get_access_token()

    if not client_id or not access_token:
        ENGINE_STATE["dhan"] = None
        return

    if dhanhq is None or DhanContext is None:
        logger.warning("dhanhq SDK not available. ENGINE_STATE['dhan'] will be None.")
        ENGINE_STATE["dhan"] = None
        return

    try:
        ENGINE_STATE["dhan"] = dhanhq(DhanContext(str(client_id), str(access_token)))
    except Exception as e:
        logger.error(f"Failed to create Dhan client: {e}")
        ENGINE_STATE["dhan"] = None


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
    except Exception as e:
        logger.warning(f"Market data cache load failed: {e}")

    # init engine state
    await _init_engine_state_stocks()
    await _init_engine_configs()
    await _init_engine_live_flags()
    await _init_dhan_client_into_engine_state()

    # start tasks
    STATE.stop_event.clear()
    STATE.dispatch_task = asyncio.create_task(tick_dispatcher(), name="tick_dispatcher")
    STATE.ws_task = asyncio.create_task(dhan_marketfeed_worker(), name="dhan_marketfeed_worker")
    STATE.order_ws_task = asyncio.create_task(dhan_order_updates_worker(), name="dhan_order_updates_worker")

    logger.info("Startup complete")


@app.on_event("shutdown")
async def on_shutdown():
    logger.info("Shutdown requested")
    STATE.stop_event.set()

    # cancel tasks
    for t in (STATE.ws_task, STATE.order_ws_task, STATE.dispatch_task):
        if t and not t.done():
            t.cancel()

    # wait tasks
    for t in (STATE.ws_task, STATE.order_ws_task, STATE.dispatch_task):
        if t:
            try:
                await t
            except Exception:
                pass

    # best-effort close websockets
    try:
        if STATE._ws is not None:
            await STATE._ws.send(json.dumps({"RequestCode": 12}))
            await STATE._ws.close()
    except Exception:
        pass

    try:
        if STATE._order_ws is not None:
            await STATE._order_ws.close()
    except Exception:
        pass

    logger.info("Shutdown complete")
