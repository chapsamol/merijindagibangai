# main.py (FULL) — low-latency integrations + BreakoutEngine + MomentumEngine (synced)
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

from redis_manager import TradeControl, get_redis
from breakout_engine import BreakoutEngine
from momentum_engine import MomentumEngine


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
# Fast JSON (orjson fallback)
# -----------------------------
try:
    import orjson  # type: ignore

    def _json_dumps(obj: Any) -> str:
        return orjson.dumps(obj).decode("utf-8")

    def _json_loads(s: str) -> Any:
        return orjson.loads(s)
except Exception:
    def _json_dumps(obj: Any) -> str:
        return json.dumps(obj)

    def _json_loads(s: str) -> Any:
        return json.loads(s)


# -----------------------------
# Config
# -----------------------------
APP_DIR = os.path.dirname(os.path.abspath(__file__))
INDEX_HTML = os.getenv("INDEX_HTML_PATH", os.path.join(APP_DIR, "index.html"))

# Dhan Marketfeed WS (v2)
DHAN_WS_BASE = os.getenv("DHAN_WS_BASE", "wss://api-feed.dhan.co")
DHAN_WS_AUTH_TYPE = os.getenv("DHAN_WS_AUTH_TYPE", "2")

# Dhan Order Update WS (separate)
DHAN_ORDER_WS = os.getenv("DHAN_ORDER_WS", "wss://api-order-update.dhan.co")

# Subscribe mode: 15=TICKER, 17=QUOTE, 21=FULL
DHAN_SUBSCRIBE_REQUEST_CODE = int(os.getenv("DHAN_SUBSCRIBE_REQUEST_CODE", "17"))

# Per-message instrument limit
DHAN_SUBSCRIBE_CHUNK = int(os.getenv("DHAN_SUBSCRIBE_CHUNK", "100"))

# Sleep between subscription batches
SUBSCRIBE_BATCH_SLEEP = float(os.getenv("SUBSCRIBE_BATCH_SLEEP", "0.05"))

# Tick queue
TICK_QUEUE_MAX = int(os.getenv("TICK_QUEUE_MAX", "20000"))

# Dispatcher batch drain (reduce event-loop overhead)
DISPATCH_BATCH_MAX = int(os.getenv("DISPATCH_BATCH_MAX", "200"))

# UI engine sides
ENGINE_SIDES = ["bull", "mom_bull", "bear", "mom_bear"]

# Candle TF minutes (BreakoutEngine needs candle closes)
CANDLE_TF_MINUTES = int(os.getenv("CANDLE_TF_MINUTES", "1"))

# WS keepalive (recommended on Heroku)
WS_PING_INTERVAL = float(os.getenv("WS_PING_INTERVAL", "20"))
WS_PING_TIMEOUT = float(os.getenv("WS_PING_TIMEOUT", "20"))

# Auto reinit dhan client every N seconds if missing (startup race / creds saved later)
DHAN_REINIT_INTERVAL_SEC = int(os.getenv("DHAN_REINIT_INTERVAL_SEC", "10"))

# ✅ Scanner requirements (your request)
SCANNER_TURNOVER_MIN = 1_00_00_000  # ₹1 Crore
SCANNER_VOL_SMA_MULT_REQ = 5.0      # volume >= 5 * SMA


# -----------------------------
# Binary parsing (Dhan WS is little-endian)
# Header: 8 bytes -> code(1), length(2), exch(1), secid(4)
# -----------------------------
HDR_STRUCT = struct.Struct("<BHBI")  # 8 bytes

# Ticker packet (code 2): ltp(float32), ltt(int32 epoch)
TICKER_STRUCT = struct.Struct("<fI")

# PrevClose packet (code 6): prev_close(float32), oi(int32)
PREVCLOSE_STRUCT = struct.Struct("<fI")

# Quote packet (code 4):
# ltp(f), ltq(h), ltt(I), atp(f), vol(I), tsq(I), tbq(I), open(f), close(f), high(f), low(f)
QUOTE_STRUCT = struct.Struct("<f h I f I I I f f f f")


def _parse_bool(v: Any) -> bool:
    """Robust bool parser (fixes bool('0') == True)."""
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


def _ist_day_key() -> str:
    return datetime.now(IST).strftime("%Y%m%d")


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
# Runtime state
# -----------------------------
class RuntimeState:
    def __init__(self) -> None:
        self.redis_ok: bool = False

        # Marketfeed status
        self.dhan_connected: bool = False
        self.dhan_last_msg_ts: float = 0.0

        # Order update status
        self.order_updates_connected: bool = False
        self.order_last_msg_ts: float = 0.0

        # Universe
        self.universe_tokens: List[int] = []
        self.universe_symbols: List[str] = []
        self.token_to_symbol: Dict[int, str] = {}

        # Engine toggles (string for UI)
        self.engine_status: Dict[str, str] = {s: "0" for s in ENGINE_SIDES}
        self.engine_updated_at: Dict[str, str] = {s: "" for s in ENGINE_SIDES}

        # UI placeholders
        self.pnl: Dict[str, float] = {s: 0.0 for s in ENGINE_SIDES}
        self.pnl["total"] = 0.0
        self.orders: Dict[str, List[Dict[str, Any]]] = {s: [] for s in ENGINE_SIDES}
        self.scanner: Dict[str, List[Dict[str, Any]]] = {s: [] for s in ENGINE_SIDES}

        # Cached market data (pdh/pdl/sma/etc)
        self.market_data: Dict[int, Dict[str, Any]] = {}

        # realtime settings version cache
        self.settings_version: Dict[str, int] = {s: 0 for s in ENGINE_SIDES}

        # scanner (sticky) day
        self.scanner_day: str = _ist_day_key()

        # asyncio
        self.stop_event = asyncio.Event()
        self.tick_queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=TICK_QUEUE_MAX)

        self.ws_task: Optional[asyncio.Task] = None
        self.order_ws_task: Optional[asyncio.Task] = None
        self.dispatch_task: Optional[asyncio.Task] = None
        self.dhan_maintainer_task: Optional[asyncio.Task] = None
        self.settings_watch_task: Optional[asyncio.Task] = None

        # WS refs for shutdown
        self._ws: Optional[WebSocketClientProtocol] = None
        self._order_ws: Optional[WebSocketClientProtocol] = None

        # dhan init tracking
        self._dhan_init_ts: float = 0.0
        self._dhan_last_client_id: str = ""
        self._dhan_last_token: str = ""


STATE = RuntimeState()


# -----------------------------
# Engine state passed to engines
# -----------------------------
ENGINE_STATE: Dict[str, Any] = {
    "stocks": {},          # token -> stock dict
    "config": {},          # side -> config dict
    "engine_live": {},     # side -> bool (MUST be bool)
    "trades": {            # engine orders lists
        "bull": [],
        "bear": [],
        "mom_bull": [],
        "mom_bear": [],
    },
    "manual_exits": set(),  # symbols to exit ASAP
    "dhan": None,           # dhanhq client instance
}

# Per-token locks (engines expect locks handled outside)
TOKEN_LOCKS: Dict[int, asyncio.Lock] = {}


# -----------------------------
# Redis / creds helpers
# -----------------------------
async def _get_redis_client():
    return await get_redis()


async def _get_dhan_creds() -> Tuple[str, str]:
    """
    Robust credential loader:
      1) TradeControl.get_config + TradeControl.get_access_token
      2) Fallback to direct redis keys used by /api/control fallback writes
    """
    client_id = ""
    access_token = ""

    # 1) TradeControl (preferred)
    try:
        cid, _ = await TradeControl.get_config()
        tok = await TradeControl.get_access_token()
        client_id = (cid or "").strip()
        access_token = (tok or "").strip()
    except Exception:
        pass

    # 2) Fallback keys
    if not client_id or not access_token:
        r = await _get_redis_client()
        if not client_id:
            try:
                v = await r.get("nexus:config:api_key")
                client_id = (v or "").strip()
            except Exception:
                pass
        if not access_token:
            try:
                v = await r.get("nexus:auth:access_token")
                access_token = (v or "").strip()
            except Exception:
                pass

        # last fallback
        if not access_token:
            try:
                v = await r.get("nexus:config:api_secret")
                access_token = (v or "").strip()
            except Exception:
                pass

    return client_id, access_token


async def _init_dhan_client_into_engine_state(force: bool = False) -> None:
    """
    ✅ FIXED for your Heroku dhanhq package:
      from dhanhq import dhanhq
      dhan = dhanhq(client_id, access_token)
    """
    now = time.time()
    if not force and ENGINE_STATE.get("dhan") is not None:
        return
    if not force and (now - (STATE._dhan_init_ts or 0.0)) < DHAN_REINIT_INTERVAL_SEC:
        return

    STATE._dhan_init_ts = now

    client_id, access_token = await _get_dhan_creds()
    if not client_id or not access_token:
        ENGINE_STATE["dhan"] = None
        logger.error("❌ Dhan creds missing (client_id/access_token) -> dhan client NOT initialized.")
        return

    # If creds changed, re-init
    if (STATE._dhan_last_client_id != client_id) or (STATE._dhan_last_token != access_token):
        STATE._dhan_last_client_id = client_id
        STATE._dhan_last_token = access_token
        ENGINE_STATE["dhan"] = None

    try:
        from dhanhq import dhanhq  # ✅ correct for your installed version
        ENGINE_STATE["dhan"] = dhanhq(str(client_id), str(access_token))
        logger.info("✅ Dhan client initialized for order placement.")
    except Exception as e:
        ENGINE_STATE["dhan"] = None
        logger.error(f"❌ Dhan SDK init failed: {e}")


async def dhan_client_maintainer() -> None:
    """
    Low-latency: keep dhan client initialized WITHOUT doing Redis calls in tick hot-path.
    """
    while not STATE.stop_event.is_set():
        try:
            await _init_dhan_client_into_engine_state(force=False)
        except Exception:
            pass
        await asyncio.sleep(max(2.0, float(DHAN_REINIT_INTERVAL_SEC)))


async def _load_universe_from_redis() -> Tuple[List[int], List[str]]:
    """
    Load universe tokens/symbols.
    Primary:
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
                    symbols = [str(x) for x in _json_loads(syms_raw)]
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
            tokens = [int(x) for x in _json_loads(tokens_raw)]
        except Exception:
            tokens = [int(x) for x in str(tokens_raw).split(",") if x.strip().isdigit()]

    if syms_raw:
        try:
            symbols = [str(x) for x in _json_loads(syms_raw)]
        except Exception:
            symbols = [x.strip().upper() for x in str(syms_raw).split(",") if x.strip()]

    return tokens, symbols


async def _load_market_data_bulk(tokens: List[int]) -> Dict[int, Dict[str, Any]]:
    """
    Load cached market data (pdh/pdl/sma/etc).
    Primary: TradeControl.get_market_data(token)
    Fallback: nexus:market:{token}
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
                out[int(t)] = _json_loads(raw)
        except Exception:
            continue

    return out


async def _load_engine_status() -> None:
    """
    Loads engine toggles from Redis.
    Also loads updated_at for UI clarity.
    """
    r = await _get_redis_client()
    for side in ENGINE_SIDES:
        v = await r.get(f"nexus:engine:{side}:enabled")
        if v is not None:
            STATE.engine_status[side] = "1" if str(v).strip().lower() in ("1", "true", "yes", "on") else "0"
        try:
            if hasattr(TradeControl, "get_engine_updated_at"):
                STATE.engine_updated_at[side] = await TradeControl.get_engine_updated_at(side)
        except Exception:
            pass


async def _save_engine_status(side: str, enabled: bool) -> None:
    """
    Single source of truth is Redis.
    Also syncs ENGINE_STATE["engine_live"] for engine logic.
    """
    try:
        if hasattr(TradeControl, "set_engine_enabled"):
            await TradeControl.set_engine_enabled(side, enabled)
        else:
            r = await _get_redis_client()
            await r.set(f"nexus:engine:{side}:enabled", "1" if enabled else "0")
    except Exception:
        r = await _get_redis_client()
        await r.set(f"nexus:engine:{side}:enabled", "1" if enabled else "0")

    STATE.engine_status[side] = "1" if enabled else "0"
    ENGINE_STATE["engine_live"][side] = bool(enabled)

    try:
        if hasattr(TradeControl, "get_engine_updated_at"):
            STATE.engine_updated_at[side] = await TradeControl.get_engine_updated_at(side)
    except Exception:
        pass


async def _load_engine_settings(side: str) -> Dict[str, Any]:
    fn = getattr(TradeControl, "get_strategy_settings", None)
    if callable(fn):
        data = await fn(side)
        return data or {}

    r = await _get_redis_client()
    raw = await r.get(f"nexus:settings:{side}")
    return _json_loads(raw) if raw else {}


async def _save_engine_settings(side: str, payload: Dict[str, Any]) -> bool:
    fn = getattr(TradeControl, "save_strategy_settings", None)
    if callable(fn):
        ok = await fn(side, payload)
        return bool(ok)

    r = await _get_redis_client()
    await r.set(f"nexus:settings:{side}", _json_dumps(payload))
    return True


async def _bump_settings_version(side: str) -> int:
    if hasattr(TradeControl, "bump_settings_version"):
        return int(await TradeControl.bump_settings_version(side))
    r = await _get_redis_client()
    k = f"nexus:settings:version:{side}"
    try:
        v = await r.incr(k)
        return int(v)
    except Exception:
        return 0


async def _get_settings_version(side: str) -> int:
    if hasattr(TradeControl, "get_settings_version"):
        return int(await TradeControl.get_settings_version(side))
    r = await _get_redis_client()
    try:
        v = await r.get(f"nexus:settings:version:{side}")
        return int(v) if v else 0
    except Exception:
        return 0


async def settings_watcher() -> None:
    """
    ✅ Real-time settings reload without restart:
    - Each /save bumps nexus:settings:version:{side}
    - Watcher pulls latest config when version changes
    """
    while not STATE.stop_event.is_set():
        try:
            for side in ENGINE_SIDES:
                v = await _get_settings_version(side)
                if int(v) != int(STATE.settings_version.get(side, 0)):
                    cfg = await _load_engine_settings(side)
                    ENGINE_STATE["config"][side] = cfg or {}
                    STATE.settings_version[side] = int(v)
        except Exception:
            pass
        await asyncio.sleep(0.6)


# -----------------------------
# Marketfeed WS
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
    batches = _chunk_instruments(tokens)
    for batch in batches:
        payload = {
            "RequestCode": DHAN_SUBSCRIBE_REQUEST_CODE,
            "InstrumentCount": len(batch),
            "InstrumentList": [{"ExchangeSegment": "NSE_EQ", "SecurityId": str(t)} for t in batch],
        }
        await ws.send(_json_dumps(payload))
        if SUBSCRIBE_BATCH_SLEEP > 0:
            await asyncio.sleep(SUBSCRIBE_BATCH_SLEEP)


def _parse_binary_message(data: bytes) -> Optional[Tick]:
    if not data or len(data) < 8:
        return None
    try:
        feed_code, _msg_len, _exch, sec_id = HDR_STRUCT.unpack_from(data, 0)
    except Exception:
        return None

    token = int(sec_id)
    symbol = STATE.token_to_symbol.get(token, str(token))

    if feed_code == 2 and len(data) >= 8 + TICKER_STRUCT.size:
        ltp, ltt = TICKER_STRUCT.unpack_from(data, 8)
        return Tick(token=token, symbol=symbol, ltp=float(ltp), ltt_epoch=int(ltt))

    if feed_code == 4 and len(data) >= 8 + QUOTE_STRUCT.size:
        (
            ltp, _ltq, ltt, _atp, vol, _tsq, _tbq,
            day_open, _day_close, day_high, day_low
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

    if feed_code == 6 and len(data) >= 8 + PREVCLOSE_STRUCT.size:
        prev_close, _oi = PREVCLOSE_STRUCT.unpack_from(data, 8)
        md = STATE.market_data.get(token) or {}
        md["prev_close"] = float(prev_close)
        STATE.market_data[token] = md

        # Sync into ENGINE_STATE["stocks"] if already initialized
        st = ENGINE_STATE.get("stocks", {}).get(token)
        if isinstance(st, dict):
            st["prev_close"] = float(prev_close)
        return None

    if feed_code == 50:
        STATE.dhan_connected = False
        return None

    return None


async def dhan_marketfeed_worker() -> None:
    backoff = 1.0
    max_backoff = 20.0

    while not STATE.stop_event.is_set():
        try:
            client_id, access_token = await _get_dhan_creds()
            if not client_id or not access_token:
                STATE.dhan_connected = False
                logger.error("Dhan marketfeed creds missing in Redis.")
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

                tq = STATE.tick_queue
                while not STATE.stop_event.is_set():
                    msg = await ws.recv()
                    STATE.dhan_last_msg_ts = time.time()
                    if isinstance(msg, bytes):
                        tick = _parse_binary_message(msg)
                        if tick:
                            try:
                                tq.put_nowait(tick)
                            except asyncio.QueueFull:
                                # Drop on overload (better than blocking)
                                pass

        except asyncio.CancelledError:
            break
        except Exception as e:
            STATE.dhan_connected = False
            STATE._ws = None
            logger.error(f"Marketfeed error: {e}")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 1.6)

    try:
        if STATE._ws is not None:
            await STATE._ws.send(_json_dumps({"RequestCode": 12}))
            await STATE._ws.close()
    except Exception:
        pass
    finally:
        STATE._ws = None
        STATE.dhan_connected = False
        logger.info("Marketfeed worker exited cleanly.")


# -----------------------------
# Order update WS
# -----------------------------
def _extract_first(payload: Any, keys: Tuple[str, ...]) -> Optional[Any]:
    if not isinstance(payload, dict):
        return None
    for k in keys:
        if k in payload and payload.get(k) is not None:
            return payload.get(k)
    return None


def _extract_order_id_any(payload: Any) -> Optional[str]:
    if payload is None:
        return None
    if isinstance(payload, dict):
        v = _extract_first(payload, ("orderId", "order_id", "OrderId", "orderNo", "OrderNo"))
        if v:
            return str(v)
        for k in ("data", "Data", "order", "Order", "result", "Result"):
            oid = _extract_order_id_any(payload.get(k))
            if oid:
                return oid
    return None


def _extract_status(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    v = _extract_first(payload, ("orderStatus", "status", "Status", "order_status"))
    if v:
        return str(v).upper()
    for k in ("data", "Data", "order", "Order", "result", "Result"):
        st = _extract_status(payload.get(k))
        if st:
            return st
    return None


def _extract_avg_price(payload: Any) -> Optional[float]:
    if not isinstance(payload, dict):
        return None
    v = _extract_first(payload, (
        "averageTradedPrice", "avgTradedPrice", "AvgTradedPrice",
        "average_price", "avg_price", "tradedPrice", "TradedPrice"
    ))
    if v is not None:
        try:
            return float(v)
        except Exception:
            pass
    for k in ("data", "Data", "order", "Order", "result", "Result"):
        sub = payload.get(k)
        if isinstance(sub, dict):
            ap = _extract_avg_price(sub)
            if ap is not None:
                return ap
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
    """
    Recompute target/trailing from executed entry price.
    FIX: supports momentum sides ("mom_bull"/"mom_bear") too.
    """
    try:
        ep = float(exec_price)
        if ep <= 0:
            return
        trade["entry_exec_price"] = ep
        trade["entry_price"] = ep

        sl = float(trade.get("sl_price") or 0.0)
        rr_val = float(trade.get("rr_val") or 2.0)
        tsl_ratio = float(trade.get("tsl_ratio") or 1.5)

        init_risk = abs(ep - sl)
        if init_risk <= 0:
            return

        trade["init_risk"] = float(init_risk)
        side_raw = str(trade.get("side") or "").lower()

        # normalize sides
        is_bull = (side_raw == "bull") or side_raw.endswith("bull")
        is_bear = (side_raw == "bear") or side_raw.endswith("bear")

        if is_bull:
            trade["target_price"] = round(ep + (init_risk * rr_val), 2)
        elif is_bear:
            trade["target_price"] = round(ep - (init_risk * rr_val), 2)

        trade["trail_step"] = float(init_risk * tsl_ratio) if tsl_ratio > 0 else float(init_risk)
    except Exception:
        return


def _sync_ui_pnl_from_trades() -> None:
    """
    Pull per-trade pnl computed by engines into STATE.pnl for UI.
    No heavy work, just summations.
    """
    try:
        trades = ENGINE_STATE.get("trades", {}) or {}

        total = 0.0
        for side in ENGINE_SIDES:
            side_pnl = 0.0
            for t in trades.get(side, []) or []:
                try:
                    side_pnl += float(t.get("pnl") or 0.0)
                except Exception:
                    continue
            STATE.pnl[side] = round(side_pnl, 2)
            total += side_pnl

        STATE.pnl["total"] = round(total, 2)
    except Exception:
        pass


def _sync_ui_orders_from_trades() -> None:
    try:
        trades = ENGINE_STATE.get("trades", {}) or {}
        for side in ENGINE_SIDES:
            if side in trades:
                STATE.orders[side] = list(trades.get(side, []))
    except Exception:
        pass
    _sync_ui_pnl_from_trades()


async def dhan_order_updates_worker() -> None:
    backoff = 1.0
    max_backoff = 20.0

    tracked_sides = ("bull", "bear", "mom_bull", "mom_bear")

    while not STATE.stop_event.is_set():
        try:
            client_id, access_token = await _get_dhan_creds()
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

                # Mandatory login/auth
                login = {
                    "LoginReq": {"MsgCode": 42, "ClientId": str(client_id), "Token": str(access_token)},
                    "UserType": "SELF",
                }
                await ws.send(_json_dumps(login))

                while not STATE.stop_event.is_set():
                    msg = await ws.recv()
                    STATE.order_last_msg_ts = time.time()

                    if not isinstance(msg, str):
                        continue

                    try:
                        payload = _json_loads(msg)
                    except Exception:
                        continue

                    oid = _extract_order_id_any(payload)
                    st = _extract_status(payload)
                    avgp = _extract_avg_price(payload)

                    if oid:
                        for side in tracked_sides:
                            trades = ENGINE_STATE["trades"].get(side, [])
                            for trade in trades:
                                if str(trade.get("order_id")) == str(oid):
                                    if avgp and _is_filled_status(st):
                                        _recompute_trade_from_exec(trade, float(avgp))
                                    if _is_rejected_status(st):
                                        trade["status"] = "REJECTED"
                                        trade["reject_reason"] = str(st or "REJECTED")
                                    break

                                if trade.get("exit_order_id") and str(trade.get("exit_order_id")) == str(oid):
                                    if avgp and _is_filled_status(st):
                                        trade["exit_exec_price"] = float(avgp)
                                        trade["status"] = "CLOSED"
                                        trade["exit_time"] = datetime.now(IST).strftime("%H:%M:%S")
                                    if _is_rejected_status(st):
                                        trade["exit_reject_reason"] = str(st or "REJECTED")
                                    break

                    _sync_ui_orders_from_trades()

        except asyncio.CancelledError:
            break
        except Exception as e:
            STATE.order_updates_connected = False
            STATE._order_ws = None
            logger.error(f"OrderUpdate WS error: {e}")
            await asyncio.sleep(backoff)
            backoff = min(max_backoff, backoff * 1.6)

    STATE.order_updates_connected = False
    STATE._order_ws = None
    logger.info("OrderUpdate worker exited cleanly.")


# -----------------------------
# ✅ Scanner (sticky) logic
# -----------------------------
def _ensure_scanner_day_rollover() -> None:
    d = _ist_day_key()
    if d != STATE.scanner_day:
        STATE.scanner_day = d
        # reset in-memory scanner lists on new IST day
        for s in ENGINE_SIDES:
            STATE.scanner[s] = []


async def _scanner_is_sticky(side: str, symbol: str) -> bool:
    # Prefer Redis membership (day-wise). If unavailable, fallback false.
    try:
        if hasattr(TradeControl, "scanner_get_sticky"):
            sticky = await TradeControl.scanner_get_sticky(side)
            return symbol.upper() in set(sticky or [])
    except Exception:
        pass
    return False


async def _scanner_load_from_redis() -> None:
    """
    On startup: load today's sticky scanner rows into STATE.scanner.
    """
    _ensure_scanner_day_rollover()
    for side in ("bull", "bear"):
        rows: Dict[str, Dict[str, Any]] = {}
        try:
            if hasattr(TradeControl, "scanner_get_rows"):
                rows = await TradeControl.scanner_get_rows(side)
        except Exception:
            rows = {}

        # convert to list, stable order: most recent update first
        lst = []
        for sym, row in (rows or {}).items():
            if isinstance(row, dict):
                row.setdefault("symbol", sym)
                lst.append(row)
        lst.sort(key=lambda x: float(x.get("last_update_epoch") or 0), reverse=True)
        STATE.scanner[side] = lst[:400]


async def _scanner_upsert(side: str, symbol: str, row: Dict[str, Any]) -> None:
    """
    Upsert row into Redis + STATE.scanner[side] (dedupe by symbol)
    """
    symbol = symbol.upper()
    row["symbol"] = symbol

    # Redis
    try:
        if hasattr(TradeControl, "scanner_add_sticky"):
            await TradeControl.scanner_add_sticky(side, symbol)
        if hasattr(TradeControl, "scanner_upsert_row"):
            await TradeControl.scanner_upsert_row(side, symbol, row)
    except Exception:
        pass

    # STATE (dedupe)
    lst = STATE.scanner.get(side) or []
    found = False
    for i in range(len(lst)):
        if str(lst[i].get("symbol", "")).upper() == symbol:
            lst[i] = row
            found = True
            break
    if not found:
        lst.insert(0, row)
    # sort by update time
    lst.sort(key=lambda x: float(x.get("last_update_epoch") or 0), reverse=True)
    STATE.scanner[side] = lst[:400]


async def _process_breakout_scanner_from_candle(token: int, candle: Dict[str, Any]) -> None:
    """
    Your rule:
      - show ONLY stocks where breakout candle turnover >= 1 crore
      - and volume SMA multiplier >= 5
      - once appears, stay all day (sticky)
      - keep updating
    """
    _ensure_scanner_day_rollover()

    st = ENGINE_STATE.get("stocks", {}).get(token)
    if not isinstance(st, dict):
        return

    symbol = str(st.get("symbol") or token).upper()
    close_px = float(candle.get("close") or 0.0)
    vol_candle = int(candle.get("volume") or 0)

    if close_px <= 0 or vol_candle <= 0:
        return

    pdh = float(st.get("pdh") or 0.0)
    pdl = float(st.get("pdl") or 0.0)
    sma = float(st.get("sma") or 0.0)  # assumed SMA volume average

    # Determine breakout side using PDH/PDL (breakout candle)
    side: Optional[str] = None
    if pdh > 0 and close_px >= pdh:
        side = "bull"
    elif pdl > 0 and close_px <= pdl:
        side = "bear"
    else:
        # not a breakout/breakdown candle
        # BUT if already sticky, keep updating on every close
        for s in ("bull", "bear"):
            if await _scanner_is_sticky(s, symbol):
                side = s
                break
        if side is None:
            return

    turnover = float(close_px) * float(vol_candle)
    vol_mult = (float(vol_candle) / float(sma)) if sma and sma > 0 else 0.0

    qualifies = (turnover >= float(SCANNER_TURNOVER_MIN)) and (vol_mult >= float(SCANNER_VOL_SMA_MULT_REQ))

    # Once sticky, keep updating even if it doesn't qualify later
    is_sticky = await _scanner_is_sticky(side, symbol)

    if not qualifies and not is_sticky:
        return

    now = datetime.now(IST)
    now_str = now.strftime("%H:%M:%S")
    now_epoch = time.time()

    # Preserve first_seen if present already in STATE
    first_seen = now_str
    first_seen_epoch = now_epoch
    for item in (STATE.scanner.get(side) or []):
        if str(item.get("symbol", "")).upper() == symbol:
            first_seen = str(item.get("first_seen") or first_seen)
            first_seen_epoch = float(item.get("first_seen_epoch") or first_seen_epoch)
            break

    reason = (
        f"TURNOVER ₹{turnover:,.0f} | VOLx {vol_mult:.2f} (SMA {sma:,.0f})"
        + (" ✅" if qualifies else " (STICKY)")
    )

    row = {
        "symbol": symbol,
        "sideId": side,
        "trigger_px": close_px,
        "seen_time": now_str,
        "first_seen": first_seen,
        "first_seen_epoch": first_seen_epoch,
        "last_update_epoch": now_epoch,
        "candle_volume": vol_candle,
        "volume_sma": sma,
        "volume_mult": round(vol_mult, 4),
        "turnover": round(turnover, 2),
        "reason": reason,
    }

    await _scanner_upsert(side, symbol, row)


# -----------------------------
# Dispatcher + Candle builder + Engines
# -----------------------------
def _bucket_epoch(epoch: int, tf_min: int) -> int:
    if epoch <= 0:
        return 0
    tf_sec = int(tf_min) * 60
    return int(epoch // tf_sec) * tf_sec


def _breakout_any_enabled() -> bool:
    return (STATE.engine_status.get("bull") == "1") or (STATE.engine_status.get("bear") == "1")


def _momentum_any_enabled() -> bool:
    return (STATE.engine_status.get("mom_bull") == "1") or (STATE.engine_status.get("mom_bear") == "1")


async def _handle_candle_close(token: int, closed: Dict[str, Any]) -> None:
    """
    Offloads candle-close work from dispatcher hot-path.
    Locks per token inside this task.
    """
    lock = TOKEN_LOCKS.get(token)
    if lock is None:
        lock = asyncio.Lock()
        TOKEN_LOCKS[token] = lock

    async with lock:
        # ✅ Engines candle-close gated by toggles (prevents accidental trading when OFF)
        if _breakout_any_enabled():
            try:
                await BreakoutEngine.on_candle_close(token, dict(closed), ENGINE_STATE)
            except Exception as e:
                logger.error(f"CandleClose BRK error token={token}: {e}")

        if _momentum_any_enabled():
            try:
                await MomentumEngine.on_candle_close(token, dict(closed), ENGINE_STATE)
            except Exception as e:
                logger.error(f"CandleClose MOM error token={token}: {e}")

        # ✅ Scanner update based on breakout candle rules
        try:
            await _process_breakout_scanner_from_candle(token, dict(closed))
        except Exception as e:
            logger.error(f"Scanner candle processing error token={token}: {e}")


async def tick_dispatcher() -> None:
    logger.info("Tick dispatcher active (candle builder + engines + sticky scanner).")

    cur_bucket: Dict[int, int] = {}
    cur_candle: Dict[int, Dict[str, Any]] = {}
    last_day_vol: Dict[int, int] = {}

    tq = STATE.tick_queue
    locks = TOKEN_LOCKS

    while not STATE.stop_event.is_set():
        try:
            tick = await tq.get()
        except asyncio.CancelledError:
            break

        # Batch drain to reduce await overhead
        ticks: List[Tick] = [tick]
        for _ in range(max(0, DISPATCH_BATCH_MAX - 1)):
            try:
                ticks.append(tq.get_nowait())
            except asyncio.QueueEmpty:
                break

        brk_enabled = _breakout_any_enabled()
        mom_enabled = _momentum_any_enabled()

        for tk in ticks:
            token = int(tk.token)
            ltp = float(tk.ltp)
            vol = int(tk.volume or 0)

            # Update ltp in stock cache (cheap)
            st = ENGINE_STATE.get("stocks", {}).get(token)
            if isinstance(st, dict):
                st["ltp"] = ltp

            # Candle builder
            bucket = _bucket_epoch(int(tk.ltt_epoch or 0), CANDLE_TF_MINUTES)

            v_delta = 0
            if tk.volume is not None:
                prev = last_day_vol.get(token)
                if prev is not None:
                    v_delta = max(0, int(tk.volume) - int(prev))
                last_day_vol[token] = int(tk.volume)

            if bucket > 0:
                prev_bucket = cur_bucket.get(token)
                if prev_bucket is None:
                    cur_bucket[token] = bucket
                    cur_candle[token] = {
                        "open": ltp,
                        "high": ltp,
                        "low": ltp,
                        "close": ltp,
                        "volume": int(v_delta),
                        "bucket_epoch": bucket,
                    }
                elif bucket == prev_bucket:
                    c = cur_candle.get(token)
                    if c:
                        if ltp > float(c["high"]):
                            c["high"] = ltp
                        if ltp < float(c["low"]):
                            c["low"] = ltp
                        c["close"] = ltp
                        c["volume"] = int(c.get("volume", 0)) + int(v_delta)
                else:
                    closed = cur_candle.get(token)
                    if closed:
                        asyncio.create_task(_handle_candle_close(token, dict(closed)))

                    cur_bucket[token] = bucket
                    cur_candle[token] = {
                        "open": ltp,
                        "high": ltp,
                        "low": ltp,
                        "close": ltp,
                        "volume": int(v_delta),
                        "bucket_epoch": bucket,
                    }

            # Tick-fast engines (hard gated)
            if not brk_enabled and not mom_enabled:
                continue

            lock = locks.get(token)
            if lock is None:
                lock = asyncio.Lock()
                locks[token] = lock

            async with lock:
                if brk_enabled:
                    try:
                        await BreakoutEngine.run(token=token, ltp=ltp, vol=vol, state=ENGINE_STATE)
                    except Exception as e:
                        logger.error(f"Breakout run error token={token}: {e}")

                if mom_enabled:
                    try:
                        await MomentumEngine.run(token=token, ltp=ltp, vol=vol, state=ENGINE_STATE)
                    except Exception as e:
                        logger.error(f"Momentum run error token={token}: {e}")

        _sync_ui_orders_from_trades()


# -----------------------------
# ✅ Square-off helpers (real)
# -----------------------------
def _mark_exit_requested(symbol: str) -> int:
    """
    Mark OPEN trades with this symbol as EXIT_REQUESTED for UI clarity.
    """
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return 0

    cnt = 0
    for side in ENGINE_SIDES:
        for t in (ENGINE_STATE.get("trades", {}).get(side, []) or []):
            try:
                if str(t.get("symbol") or "").upper() == symbol:
                    st = str(t.get("status") or "").upper()
                    if st in ("OPEN", "RUNNING", ""):
                        t["status"] = "EXIT_REQUESTED"
                        t["exit_request_time"] = datetime.now(IST).strftime("%H:%M:%S")
                        cnt += 1
            except Exception:
                continue
    return cnt


def _request_exit_symbol(symbol: str) -> int:
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return 0
    ENGINE_STATE["manual_exits"].add(symbol)
    return _mark_exit_requested(symbol)


def _request_exit_side(side: str) -> int:
    side = str(side or "").strip()
    if side not in ENGINE_SIDES:
        return 0
    cnt = 0
    for t in (ENGINE_STATE.get("trades", {}).get(side, []) or []):
        sym = str(t.get("symbol") or "").strip().upper()
        st = str(t.get("status") or "").upper()
        if sym and st in ("OPEN", "RUNNING", ""):
            ENGINE_STATE["manual_exits"].add(sym)
            t["status"] = "EXIT_REQUESTED"
            t["exit_request_time"] = datetime.now(IST).strftime("%H:%M:%S")
            cnt += 1
    return cnt


def _request_exit_all() -> int:
    cnt = 0
    for s in ENGINE_SIDES:
        cnt += _request_exit_side(s)
    return cnt


# -----------------------------
# FastAPI
# -----------------------------
app = FastAPI(title="Nexus Core", version="2.3")

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
        "breakout": _breakout_any_enabled(),
        "momentum": _momentum_any_enabled(),
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
        "engine_updated_at": STATE.engine_updated_at,
        "data_connected": data_connected,
        "pnl": STATE.pnl,
        "dhan_client_ready": bool(ENGINE_STATE.get("dhan") is not None),
        "settings_version": STATE.settings_version,
    }


@app.get("/api/orders")
async def api_orders():
    _sync_ui_orders_from_trades()
    return STATE.orders


@app.get("/api/scanner")
async def api_scanner():
    # Only sticky scanner results (as requested)
    return STATE.scanner


@app.get("/api/config/auth")
async def api_config_auth():
    client_id, access_token = await _get_dhan_creds()
    return {"client_id": client_id or "", "access_token": access_token or ""}


@app.post("/api/settings/engine/{side}")
async def save_engine_settings(side: str, payload: Dict[str, Any]):
    if side not in ENGINE_SIDES:
        raise HTTPException(status_code=400, detail="Invalid side")

    ok = await _save_engine_settings(side, payload)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to save settings")

    # ✅ Sync config for ALL sides immediately (real-time effect)
    ENGINE_STATE["config"][side] = payload or {}

    # ✅ bump version so watcher + other processes refresh immediately
    v = await _bump_settings_version(side)
    STATE.settings_version[side] = int(v)

    return {"status": "success", "settings_version": int(v)}


@app.get("/api/settings/engine/{side}")
async def get_engine_settings(side: str):
    if side not in ENGINE_SIDES:
        raise HTTPException(status_code=400, detail="Invalid side")
    return await _load_engine_settings(side)


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

        # Re-init immediately
        await _init_dhan_client_into_engine_state(force=True)
        return {"status": "success", "dhan_client_ready": bool(ENGINE_STATE.get("dhan") is not None)}

    # ✅ Individual squareoff: request exit (real) instead of UI-only delete
    if action in ("manual_exit_one", "square_off_one"):
        symbol = (payload.get("symbol") or "").strip().upper()
        affected = 0
        if symbol:
            affected = _request_exit_symbol(symbol)
        return {"status": "success", "symbol": symbol, "affected_trades": affected}

    # ✅ Side exit-all
    if action in ("manual_exit_all", "square_off_all"):
        side = payload.get("side")
        if side:
            affected = _request_exit_side(str(side))
        else:
            affected = _request_exit_all()
        return {"status": "success", "affected_trades": int(affected)}

    # ✅ Universal squareoff (all engines)
    if action == "square_off_universal":
        affected = _request_exit_all()
        return {"status": "success", "affected_trades": int(affected)}

    raise HTTPException(status_code=400, detail="Unknown action")


# -----------------------------
# Startup helpers (engine init)
# -----------------------------
async def _init_engine_live_flags() -> None:
    for side in ENGINE_SIDES:
        ENGINE_STATE["engine_live"][side] = (STATE.engine_status.get(side) == "1")


async def _init_engine_configs() -> None:
    # Load configs for all engines
    for side in ENGINE_SIDES:
        ENGINE_STATE["config"][side] = await _load_engine_settings(side)
        # initialize settings version cache
        try:
            STATE.settings_version[side] = await _get_settings_version(side)
        except Exception:
            pass


async def _init_engine_stocks() -> None:
    stocks: Dict[int, Dict[str, Any]] = {}

    for t in STATE.universe_tokens:
        tok = int(t)
        md = STATE.market_data.get(tok, {}) or {}
        sym = STATE.token_to_symbol.get(tok, str(tok))

        stocks[tok] = {
            "token": tok,
            "security_id": tok,  # token == security_id
            "symbol": sym,

            # cached market data
            "pdh": float(md.get("pdh") or 0.0),
            "pdl": float(md.get("pdl") or 0.0),
            "sma": float(md.get("sma") or 0.0),
            "prev_close": float(md.get("prev_close") or 0.0),

            # live
            "ltp": 0.0,

            # breakout engine
            "brk_status": "WAITING",

            # momentum engine
            "mom_status": "WAITING",
            "mom_skip_today": False,
            "mom_first_day": None,
        }

        TOKEN_LOCKS.setdefault(tok, asyncio.Lock())

    ENGINE_STATE["stocks"] = stocks


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

    # engine toggles
    try:
        await _load_engine_status()
    except Exception:
        pass

    # universe
    try:
        tokens, symbols = await _load_universe_from_redis()
        STATE.universe_tokens = tokens
        STATE.universe_symbols = symbols
        STATE.token_to_symbol = {int(t): symbols[i] for i, t in enumerate(tokens) if i < len(symbols)}
        logger.info(f"Universe loaded. tokens={len(tokens)}")
    except Exception as e:
        logger.error(f"Universe load failed: {e}")

    # market data cache
    try:
        if STATE.universe_tokens:
            STATE.market_data = await _load_market_data_bulk(STATE.universe_tokens)
            logger.info(f"Market data cached. tokens={len(STATE.market_data)}")
    except Exception as e:
        logger.warning(f"Market data cache load failed: {e}")

    # init engine state
    await _init_engine_live_flags()
    await _init_engine_configs()
    await _init_engine_stocks()

    # init dhan client (might still be None if creds not saved yet; maintainer will fix)
    await _init_dhan_client_into_engine_state(force=True)

    # load sticky scanner
    try:
        await _scanner_load_from_redis()
    except Exception:
        pass

    # tasks
    STATE.stop_event.clear()
    STATE.dispatch_task = asyncio.create_task(tick_dispatcher(), name="tick_dispatcher")
    STATE.ws_task = asyncio.create_task(dhan_marketfeed_worker(), name="dhan_marketfeed_worker")
    STATE.order_ws_task = asyncio.create_task(dhan_order_updates_worker(), name="dhan_order_updates_worker")
    STATE.dhan_maintainer_task = asyncio.create_task(dhan_client_maintainer(), name="dhan_client_maintainer")
    STATE.settings_watch_task = asyncio.create_task(settings_watcher(), name="settings_watcher")

    logger.info("Startup complete")


@app.on_event("shutdown")
async def on_shutdown():
    logger.info("Shutdown requested")
    STATE.stop_event.set()

    for t in (STATE.ws_task, STATE.order_ws_task, STATE.dispatch_task, STATE.dhan_maintainer_task, STATE.settings_watch_task):
        if t and not t.done():
            t.cancel()

    for t in (STATE.ws_task, STATE.order_ws_task, STATE.dispatch_task, STATE.dhan_maintainer_task, STATE.settings_watch_task):
        if t:
            try:
                await t
            except Exception:
                pass

    try:
        if STATE._ws is not None:
            await STATE._ws.send(_json_dumps({"RequestCode": 12}))
            await STATE._ws.close()
    except Exception:
        pass

    try:
        if STATE._order_ws is not None:
            await STATE._order_ws.close()
    except Exception:
        pass

    logger.info("Shutdown complete")
