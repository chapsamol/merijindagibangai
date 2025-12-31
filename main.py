

# import os
# import json
# import time
# import struct
# import asyncio
# import logging
# from dataclasses import dataclass
# from datetime import datetime
# from typing import Any, Dict, List, Optional, Tuple

# import pytz
# import websockets
# from websockets import WebSocketClientProtocol

# from fastapi import FastAPI, HTTPException
# from fastapi.responses import FileResponse, JSONResponse
# from fastapi.middleware.cors import CORSMiddleware

# from redis_manager import TradeControl, get_redis
# from breakout_engine import BreakoutEngine


# # -----------------------------
# # Logging
# # -----------------------------
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [MAIN] %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
# )
# logger = logging.getLogger("main")

# IST = pytz.timezone("Asia/Kolkata")


# # -----------------------------
# # Config
# # -----------------------------
# APP_DIR = os.path.dirname(os.path.abspath(__file__))
# INDEX_HTML = os.getenv("INDEX_HTML_PATH", os.path.join(APP_DIR, "index.html"))

# # Dhan Marketfeed WS (v2)
# DHAN_WS_BASE = os.getenv("DHAN_WS_BASE", "wss://api-feed.dhan.co")
# DHAN_WS_AUTH_TYPE = os.getenv("DHAN_WS_AUTH_TYPE", "2")

# # Dhan Order Update WS (separate)
# DHAN_ORDER_WS = os.getenv("DHAN_ORDER_WS", "wss://api-order-update.dhan.co")

# # Subscribe mode: 15=TICKER, 17=QUOTE, 21=FULL
# DHAN_SUBSCRIBE_REQUEST_CODE = int(os.getenv("DHAN_SUBSCRIBE_REQUEST_CODE", "17"))

# # Per-message instrument limit
# DHAN_SUBSCRIBE_CHUNK = int(os.getenv("DHAN_SUBSCRIBE_CHUNK", "100"))

# # Sleep between subscription batches
# SUBSCRIBE_BATCH_SLEEP = float(os.getenv("SUBSCRIBE_BATCH_SLEEP", "0.05"))

# # Tick queue
# TICK_QUEUE_MAX = int(os.getenv("TICK_QUEUE_MAX", "20000"))

# # UI engine sides
# ENGINE_SIDES = ["bull", "mom_bull", "bear", "mom_bear"]

# # Candle TF minutes (BreakoutEngine needs candle closes)
# CANDLE_TF_MINUTES = int(os.getenv("CANDLE_TF_MINUTES", "1"))

# # WS keepalive (recommended on Heroku)
# WS_PING_INTERVAL = float(os.getenv("WS_PING_INTERVAL", "20"))
# WS_PING_TIMEOUT = float(os.getenv("WS_PING_TIMEOUT", "20"))

# # Auto reinit dhan client every N seconds if missing (startup race / creds saved later)
# DHAN_REINIT_INTERVAL_SEC = int(os.getenv("DHAN_REINIT_INTERVAL_SEC", "10"))


# # -----------------------------
# # Binary parsing (Dhan WS is little-endian)
# # Header: 8 bytes -> code(1), length(2), exch(1), secid(4)
# # -----------------------------
# HDR_STRUCT = struct.Struct("<BHBI")  # 8 bytes

# # Ticker packet (code 2): ltp(float32), ltt(int32 epoch)
# TICKER_STRUCT = struct.Struct("<fI")

# # PrevClose packet (code 6): prev_close(float32), oi(int32)
# PREVCLOSE_STRUCT = struct.Struct("<fI")

# # Quote packet (code 4):
# # ltp(f), ltq(h), ltt(I), atp(f), vol(I), tsq(I), tbq(I), open(f), close(f), high(f), low(f)
# QUOTE_STRUCT = struct.Struct("<f h I f I I I f f f f")


# def _parse_bool(v: Any) -> bool:
#     """Robust bool parser (fixes bool('0') == True)."""
#     if isinstance(v, bool):
#         return v
#     if v is None:
#         return False
#     if isinstance(v, (int, float)):
#         return int(v) != 0
#     if isinstance(v, str):
#         s = v.strip().lower()
#         if s in ("1", "true", "yes", "on"):
#             return True
#         if s in ("0", "false", "no", "off", ""):
#             return False
#     return False


# @dataclass(frozen=True)
# class Tick:
#     token: int
#     symbol: str
#     ltp: float
#     ltt_epoch: int
#     volume: Optional[int] = None
#     open: Optional[float] = None
#     high: Optional[float] = None
#     low: Optional[float] = None
#     prev_close: Optional[float] = None


# # -----------------------------
# # Runtime state
# # -----------------------------
# class RuntimeState:
#     def __init__(self) -> None:
#         self.redis_ok: bool = False

#         # Marketfeed status
#         self.dhan_connected: bool = False
#         self.dhan_last_msg_ts: float = 0.0

#         # Order update status
#         self.order_updates_connected: bool = False
#         self.order_last_msg_ts: float = 0.0

#         # Universe
#         self.universe_tokens: List[int] = []
#         self.universe_symbols: List[str] = []
#         self.token_to_symbol: Dict[int, str] = {}

#         # Engine toggles (string for UI)
#         self.engine_status: Dict[str, str] = {s: "0" for s in ENGINE_SIDES}

#         # UI placeholders
#         self.pnl: Dict[str, float] = {s: 0.0 for s in ENGINE_SIDES}
#         self.pnl["total"] = 0.0
#         self.orders: Dict[str, List[Dict[str, Any]]] = {s: [] for s in ENGINE_SIDES}
#         self.scanner: Dict[str, List[Dict[str, Any]]] = {s: [] for s in ENGINE_SIDES}

#         # Cached market data (pdh/pdl/sma/etc)
#         self.market_data: Dict[int, Dict[str, Any]] = {}

#         # asyncio
#         self.stop_event = asyncio.Event()
#         self.tick_queue: asyncio.Queue[Tick] = asyncio.Queue(maxsize=TICK_QUEUE_MAX)

#         self.ws_task: Optional[asyncio.Task] = None
#         self.order_ws_task: Optional[asyncio.Task] = None
#         self.dispatch_task: Optional[asyncio.Task] = None

#         # WS refs for shutdown
#         self._ws: Optional[WebSocketClientProtocol] = None
#         self._order_ws: Optional[WebSocketClientProtocol] = None

#         # dhan init tracking
#         self._dhan_init_ts: float = 0.0
#         self._dhan_last_client_id: str = ""
#         self._dhan_last_token: str = ""


# STATE = RuntimeState()


# # -----------------------------
# # Engine state passed to BreakoutEngine
# # -----------------------------
# ENGINE_STATE: Dict[str, Any] = {
#     "stocks": {},          # token -> stock dict
#     "config": {},          # side -> config dict
#     "engine_live": {},     # side -> bool (MUST be bool)
#     "trades": {"bull": [], "bear": []},
#     "manual_exits": set(),
#     "dhan": None,          # dhanhq client instance
# }

# # Per-token locks (BreakoutEngine expects locks handled outside)
# TOKEN_LOCKS: Dict[int, asyncio.Lock] = {}


# # -----------------------------
# # Redis / creds helpers
# # -----------------------------
# async def _get_redis_client():
#     return await get_redis()


# async def _get_dhan_creds() -> Tuple[str, str]:
#     """
#     Robust credential loader:
#       1) TradeControl.get_config + TradeControl.get_access_token
#       2) Fallback to direct redis keys used by /api/control fallback writes
#     """
#     client_id = ""
#     access_token = ""

#     # 1) TradeControl (preferred)
#     try:
#         cid, _ = await TradeControl.get_config()
#         tok = await TradeControl.get_access_token()
#         client_id = (cid or "").strip()
#         access_token = (tok or "").strip()
#     except Exception:
#         pass

#     # 2) Fallback keys
#     if not client_id or not access_token:
#         r = await _get_redis_client()
#         if not client_id:
#             try:
#                 v = await r.get("nexus:config:api_key")
#                 client_id = (v or "").strip()
#             except Exception:
#                 pass
#         if not access_token:
#             try:
#                 v = await r.get("nexus:auth:access_token")
#                 access_token = (v or "").strip()
#             except Exception:
#                 pass

#         # last fallback
#         if not access_token:
#             try:
#                 v = await r.get("nexus:config:api_secret")
#                 access_token = (v or "").strip()
#             except Exception:
#                 pass

#     return client_id, access_token


# async def _init_dhan_client_into_engine_state(force: bool = False) -> None:
#     """
#     ✅ FIXED for your Heroku dhanhq package:
#       from dhanhq import dhanhq
#       dhan = dhanhq(client_id, access_token)
#     """
#     now = time.time()
#     if not force and ENGINE_STATE.get("dhan") is not None:
#         return
#     if not force and (now - (STATE._dhan_init_ts or 0.0)) < DHAN_REINIT_INTERVAL_SEC:
#         return

#     STATE._dhan_init_ts = now

#     client_id, access_token = await _get_dhan_creds()
#     if not client_id or not access_token:
#         ENGINE_STATE["dhan"] = None
#         logger.error("❌ Dhan creds missing (client_id/access_token) -> dhan client NOT initialized.")
#         return

#     # If creds changed, re-init
#     if (STATE._dhan_last_client_id != client_id) or (STATE._dhan_last_token != access_token):
#         STATE._dhan_last_client_id = client_id
#         STATE._dhan_last_token = access_token
#         ENGINE_STATE["dhan"] = None

#     try:
#         from dhanhq import dhanhq  # ✅ correct for your installed version
#         ENGINE_STATE["dhan"] = dhanhq(str(client_id), str(access_token))
#         logger.info("✅ Dhan client initialized for order placement.")
#     except Exception as e:
#         ENGINE_STATE["dhan"] = None
#         logger.error(f"❌ Dhan SDK init failed: {e}")


# async def _load_universe_from_redis() -> Tuple[List[int], List[str]]:
#     """
#     Load universe tokens/symbols.
#     Primary:
#       nexus:universe:tokens
#       nexus:universe:symbols
#     """
#     try:
#         fn_tokens = getattr(TradeControl, "get_subscribe_universe_tokens", None)
#         if callable(fn_tokens):
#             tokens = await fn_tokens()
#             tokens = [int(x) for x in (tokens or [])]
#             r = await _get_redis_client()
#             syms_raw = await r.get("nexus:universe:symbols")
#             symbols: List[str] = []
#             if syms_raw:
#                 try:
#                     symbols = [str(x) for x in json.loads(syms_raw)]
#                 except Exception:
#                     symbols = [x.strip().upper() for x in str(syms_raw).split(",") if x.strip()]
#             return tokens, symbols
#     except Exception:
#         pass

#     r = await _get_redis_client()
#     tokens_raw = await r.get("nexus:universe:tokens")
#     syms_raw = await r.get("nexus:universe:symbols")

#     tokens: List[int] = []
#     symbols: List[str] = []

#     if tokens_raw:
#         try:
#             tokens = [int(x) for x in json.loads(tokens_raw)]
#         except Exception:
#             tokens = [int(x) for x in str(tokens_raw).split(",") if x.strip().isdigit()]

#     if syms_raw:
#         try:
#             symbols = [str(x) for x in json.loads(syms_raw)]
#         except Exception:
#             symbols = [x.strip().upper() for x in str(syms_raw).split(",") if x.strip()]

#     return tokens, symbols


# async def _load_market_data_bulk(tokens: List[int]) -> Dict[int, Dict[str, Any]]:
#     """
#     Load cached market data (pdh/pdl/sma/etc).
#     Primary: TradeControl.get_market_data(token)
#     Fallback: nexus:market:{token}
#     """
#     out: Dict[int, Dict[str, Any]] = {}
#     r = await _get_redis_client()
#     get_md = getattr(TradeControl, "get_market_data", None)

#     for t in tokens:
#         try:
#             if callable(get_md):
#                 md = await get_md(str(t))
#                 if md:
#                     out[int(t)] = md
#                     continue

#             raw = await r.get(f"nexus:market:{t}")
#             if raw:
#                 out[int(t)] = json.loads(raw)
#         except Exception:
#             continue

#     return out


# async def _load_engine_status() -> None:
#     r = await _get_redis_client()
#     for side in ENGINE_SIDES:
#         v = await r.get(f"nexus:engine:{side}:enabled")
#         if v is not None:
#             STATE.engine_status[side] = "1" if str(v).strip().lower() in ("1", "true", "yes", "on") else "0"


# async def _save_engine_status(side: str, enabled: bool) -> None:
#     r = await _get_redis_client()
#     await r.set(f"nexus:engine:{side}:enabled", "1" if enabled else "0")
#     STATE.engine_status[side] = "1" if enabled else "0"

#     # Keep ENGINE_STATE synced (Breakout uses bools)
#     if side in ("bull", "bear"):
#         ENGINE_STATE["engine_live"][side] = bool(enabled)


# async def _load_engine_settings(side: str) -> Dict[str, Any]:
#     fn = getattr(TradeControl, "get_strategy_settings", None)
#     if callable(fn):
#         data = await fn(side)
#         return data or {}

#     r = await _get_redis_client()
#     raw = await r.get(f"nexus:settings:{side}")
#     return json.loads(raw) if raw else {}


# async def _save_engine_settings(side: str, payload: Dict[str, Any]) -> bool:
#     fn = getattr(TradeControl, "save_strategy_settings", None)
#     if callable(fn):
#         ok = await fn(side, payload)
#         return bool(ok)

#     r = await _get_redis_client()
#     await r.set(f"nexus:settings:{side}", json.dumps(payload))
#     return True


# # -----------------------------
# # Marketfeed WS
# # -----------------------------
# def _build_dhan_ws_url(access_token: str, client_id: str) -> str:
#     return (
#         f"{DHAN_WS_BASE}"
#         f"?version=2"
#         f"&token={access_token}"
#         f"&clientId={client_id}"
#         f"&authType={DHAN_WS_AUTH_TYPE}"
#     )


# def _chunk_instruments(tokens: List[int]) -> List[List[int]]:
#     return [tokens[i: i + DHAN_SUBSCRIBE_CHUNK] for i in range(0, len(tokens), DHAN_SUBSCRIBE_CHUNK)]


# async def _send_subscriptions(ws: WebSocketClientProtocol, tokens: List[int]) -> None:
#     batches = _chunk_instruments(tokens)
#     for batch in batches:
#         payload = {
#             "RequestCode": DHAN_SUBSCRIBE_REQUEST_CODE,
#             "InstrumentCount": len(batch),
#             "InstrumentList": [{"ExchangeSegment": "NSE_EQ", "SecurityId": str(t)} for t in batch],
#         }
#         await ws.send(json.dumps(payload))
#         if SUBSCRIBE_BATCH_SLEEP > 0:
#             await asyncio.sleep(SUBSCRIBE_BATCH_SLEEP)


# def _parse_binary_message(data: bytes) -> Optional[Tick]:
#     if not data or len(data) < 8:
#         return None
#     try:
#         feed_code, msg_len, exch, sec_id = HDR_STRUCT.unpack_from(data, 0)
#     except Exception:
#         return None

#     token = int(sec_id)
#     symbol = STATE.token_to_symbol.get(token, str(token))

#     if feed_code == 2 and len(data) >= 8 + TICKER_STRUCT.size:
#         ltp, ltt = TICKER_STRUCT.unpack_from(data, 8)
#         return Tick(token=token, symbol=symbol, ltp=float(ltp), ltt_epoch=int(ltt))

#     if feed_code == 4 and len(data) >= 8 + QUOTE_STRUCT.size:
#         (
#             ltp, ltq, ltt, atp, vol, tsq, tbq,
#             day_open, day_close, day_high, day_low
#         ) = QUOTE_STRUCT.unpack_from(data, 8)
#         return Tick(
#             token=token,
#             symbol=symbol,
#             ltp=float(ltp),
#             ltt_epoch=int(ltt),
#             volume=int(vol),
#             open=float(day_open),
#             high=float(day_high),
#             low=float(day_low),
#         )

#     if feed_code == 6 and len(data) >= 8 + PREVCLOSE_STRUCT.size:
#         prev_close, oi = PREVCLOSE_STRUCT.unpack_from(data, 8)
#         md = STATE.market_data.get(token) or {}
#         md["prev_close"] = float(prev_close)
#         STATE.market_data[token] = md
#         return None

#     if feed_code == 50:
#         STATE.dhan_connected = False
#         return None

#     return None


# async def dhan_marketfeed_worker() -> None:
#     backoff = 1.0
#     max_backoff = 20.0

#     while not STATE.stop_event.is_set():
#         try:
#             client_id, access_token = await _get_dhan_creds()
#             if not client_id or not access_token:
#                 STATE.dhan_connected = False
#                 logger.error("Dhan marketfeed creds missing in Redis.")
#                 await asyncio.sleep(2.0)
#                 continue

#             url = _build_dhan_ws_url(access_token, client_id)
#             logger.info("Marketfeed worker connecting...")

#             async with websockets.connect(
#                 url,
#                 ping_interval=WS_PING_INTERVAL,
#                 ping_timeout=WS_PING_TIMEOUT,
#                 close_timeout=2,
#                 max_queue=None,
#                 compression=None,
#             ) as ws:
#                 STATE._ws = ws
#                 STATE.dhan_connected = True
#                 STATE.dhan_last_msg_ts = time.time()
#                 backoff = 1.0

#                 if STATE.universe_tokens:
#                     await _send_subscriptions(ws, STATE.universe_tokens)
#                     logger.info(f"Subscribed tokens={len(STATE.universe_tokens)} in batches of {DHAN_SUBSCRIBE_CHUNK}.")
#                 else:
#                     logger.warning("Universe is empty; no subscriptions sent.")

#                 while not STATE.stop_event.is_set():
#                     msg = await ws.recv()
#                     STATE.dhan_last_msg_ts = time.time()
#                     if isinstance(msg, bytes):
#                         tick = _parse_binary_message(msg)
#                         if tick:
#                             try:
#                                 STATE.tick_queue.put_nowait(tick)
#                             except asyncio.QueueFull:
#                                 pass

#         except asyncio.CancelledError:
#             break
#         except Exception as e:
#             STATE.dhan_connected = False
#             STATE._ws = None
#             logger.error(f"Marketfeed error: {e}")
#             await asyncio.sleep(backoff)
#             backoff = min(max_backoff, backoff * 1.6)

#     try:
#         if STATE._ws is not None:
#             await STATE._ws.send(json.dumps({"RequestCode": 12}))
#             await STATE._ws.close()
#     except Exception:
#         pass
#     finally:
#         STATE._ws = None
#         STATE.dhan_connected = False
#         logger.info("Marketfeed worker exited cleanly.")


# # -----------------------------
# # Order update WS
# # -----------------------------
# def _extract_first(payload: Any, keys: Tuple[str, ...]) -> Optional[Any]:
#     if not isinstance(payload, dict):
#         return None
#     for k in keys:
#         if k in payload and payload.get(k) is not None:
#             return payload.get(k)
#     return None


# def _extract_order_id_any(payload: Any) -> Optional[str]:
#     if payload is None:
#         return None
#     if isinstance(payload, dict):
#         v = _extract_first(payload, ("orderId", "order_id", "OrderId", "orderNo", "OrderNo"))
#         if v:
#             return str(v)
#         for k in ("data", "Data", "order", "Order", "result", "Result"):
#             oid = _extract_order_id_any(payload.get(k))
#             if oid:
#                 return oid
#     return None


# def _extract_status(payload: Any) -> Optional[str]:
#     if not isinstance(payload, dict):
#         return None
#     v = _extract_first(payload, ("orderStatus", "status", "Status", "order_status"))
#     if v:
#         return str(v).upper()
#     for k in ("data", "Data", "order", "Order", "result", "Result"):
#         st = _extract_status(payload.get(k))
#         if st:
#             return st
#     return None


# def _extract_avg_price(payload: Any) -> Optional[float]:
#     if not isinstance(payload, dict):
#         return None
#     v = _extract_first(payload, (
#         "averageTradedPrice", "avgTradedPrice", "AvgTradedPrice",
#         "average_price", "avg_price", "tradedPrice", "TradedPrice"
#     ))
#     if v is not None:
#         try:
#             return float(v)
#         except Exception:
#             pass
#     for k in ("data", "Data", "order", "Order", "result", "Result"):
#         sub = payload.get(k)
#         if isinstance(sub, dict):
#             ap = _extract_avg_price(sub)
#             if ap is not None:
#                 return ap
#     return None


# def _is_filled_status(st: Optional[str]) -> bool:
#     if not st:
#         return False
#     s = st.upper()
#     return any(x in s for x in ("TRADED", "COMPLETE", "FILLED", "EXECUTED"))


# def _is_rejected_status(st: Optional[str]) -> bool:
#     if not st:
#         return False
#     s = st.upper()
#     return any(x in s for x in ("REJECT", "CANCEL"))


# def _recompute_trade_from_exec(trade: Dict[str, Any], exec_price: float) -> None:
#     """Recompute target/trailing from executed entry price."""
#     try:
#         ep = float(exec_price)
#         if ep <= 0:
#             return
#         trade["entry_exec_price"] = ep
#         trade["entry_price"] = ep

#         sl = float(trade.get("sl_price") or 0.0)
#         rr_val = float(trade.get("rr_val") or 2.0)
#         tsl_ratio = float(trade.get("tsl_ratio") or 1.5)

#         init_risk = abs(ep - sl)
#         if init_risk <= 0:
#             return

#         trade["init_risk"] = float(init_risk)
#         side = str(trade.get("side") or "").lower()

#         if side == "bull":
#             trade["target_price"] = round(ep + (init_risk * rr_val), 2)
#         elif side == "bear":
#             trade["target_price"] = round(ep - (init_risk * rr_val), 2)

#         trade["trail_step"] = float(init_risk * tsl_ratio) if tsl_ratio > 0 else float(init_risk)
#     except Exception:
#         return


# def _sync_ui_orders_from_trades() -> None:
#     try:
#         STATE.orders["bull"] = list(ENGINE_STATE["trades"].get("bull", []))
#         STATE.orders["bear"] = list(ENGINE_STATE["trades"].get("bear", []))
#     except Exception:
#         pass


# async def dhan_order_updates_worker() -> None:
#     backoff = 1.0
#     max_backoff = 20.0

#     while not STATE.stop_event.is_set():
#         try:
#             client_id, access_token = await _get_dhan_creds()
#             if not client_id or not access_token:
#                 STATE.order_updates_connected = False
#                 await asyncio.sleep(2.0)
#                 continue

#             logger.info("OrderUpdate worker connecting...")

#             async with websockets.connect(
#                 DHAN_ORDER_WS,
#                 ping_interval=WS_PING_INTERVAL,
#                 ping_timeout=WS_PING_TIMEOUT,
#                 close_timeout=2,
#                 max_queue=None,
#                 compression=None,
#             ) as ws:
#                 STATE._order_ws = ws
#                 STATE.order_updates_connected = True
#                 STATE.order_last_msg_ts = time.time()
#                 backoff = 1.0

#                 # Mandatory login/auth
#                 login = {
#                     "LoginReq": {"MsgCode": 42, "ClientId": str(client_id), "Token": str(access_token)},
#                     "UserType": "SELF",
#                 }
#                 await ws.send(json.dumps(login))

#                 while not STATE.stop_event.is_set():
#                     msg = await ws.recv()
#                     STATE.order_last_msg_ts = time.time()

#                     if not isinstance(msg, str):
#                         continue

#                     try:
#                         payload = json.loads(msg)
#                     except Exception:
#                         continue

#                     oid = _extract_order_id_any(payload)
#                     st = _extract_status(payload)
#                     avgp = _extract_avg_price(payload)

#                     if oid:
#                         for side in ("bull", "bear"):
#                             trades = ENGINE_STATE["trades"].get(side, [])
#                             for trade in trades:
#                                 if str(trade.get("order_id")) == str(oid):
#                                     if avgp and _is_filled_status(st):
#                                         _recompute_trade_from_exec(trade, float(avgp))
#                                     if _is_rejected_status(st):
#                                         trade["status"] = "REJECTED"
#                                         trade["reject_reason"] = str(st or "REJECTED")
#                                     break

#                                 if trade.get("exit_order_id") and str(trade.get("exit_order_id")) == str(oid):
#                                     if avgp and _is_filled_status(st):
#                                         trade["exit_exec_price"] = float(avgp)
#                                         trade["status"] = "CLOSED"
#                                         trade["exit_time"] = datetime.now(IST).strftime("%H:%M:%S")
#                                     if _is_rejected_status(st):
#                                         trade["exit_reject_reason"] = str(st or "REJECTED")
#                                     break

#                     _sync_ui_orders_from_trades()

#         except asyncio.CancelledError:
#             break
#         except Exception as e:
#             STATE.order_updates_connected = False
#             STATE._order_ws = None
#             logger.error(f"OrderUpdate WS error: {e}")
#             await asyncio.sleep(backoff)
#             backoff = min(max_backoff, backoff * 1.6)

#     STATE.order_updates_connected = False
#     STATE._order_ws = None
#     logger.info("OrderUpdate worker exited cleanly.")


# # -----------------------------
# # Dispatcher + Scanner + Candle builder + BreakoutEngine
# # -----------------------------
# def _push_scanner_signal(side: str, symbol: str, trigger_px: float, reason: str) -> None:
#     lst = STATE.scanner.get(side) or []
#     lst.insert(
#         0,
#         {
#             "symbol": symbol,
#             "trigger_px": float(trigger_px),
#             "seen_time": time.strftime("%H:%M:%S", time.localtime()),
#             "reason": reason,
#         },
#     )
#     STATE.scanner[side] = lst[:200]


# def _bucket_epoch(epoch: int, tf_min: int) -> int:
#     if epoch <= 0:
#         return 0
#     tf_sec = int(tf_min) * 60
#     return int(epoch // tf_sec) * tf_sec


# async def tick_dispatcher() -> None:
#     logger.info("Tick dispatcher active (scanner + candles + breakout engine).")

#     cur_bucket: Dict[int, int] = {}
#     cur_candle: Dict[int, Dict[str, Any]] = {}

#     # estimate per-minute volume from day volume (if available)
#     last_day_vol: Dict[int, int] = {}

#     while not STATE.stop_event.is_set():
#         try:
#             tick = await STATE.tick_queue.get()
#         except asyncio.CancelledError:
#             break

#         # Ensure dhan client exists (fixes "dhan session missing")
#         await _init_dhan_client_into_engine_state(force=False)

#         # Scanner PDH/PDL
#         md = STATE.market_data.get(tick.token) or {}
#         pdh = float(md.get("pdh") or 0.0)
#         pdl = float(md.get("pdl") or 0.0)

#         if pdh > 0 and tick.ltp >= pdh:
#             _push_scanner_signal("bull", tick.symbol, tick.ltp, "LTP >= PDH")
#         if pdl > 0 and tick.ltp <= pdl:
#             _push_scanner_signal("bear", tick.symbol, tick.ltp, "LTP <= PDL")

#         # Candle builder
#         bucket = _bucket_epoch(int(tick.ltt_epoch or 0), CANDLE_TF_MINUTES)

#         v_delta = 0
#         if tick.volume is not None:
#             prev = last_day_vol.get(tick.token)
#             if prev is not None:
#                 v_delta = max(0, int(tick.volume) - int(prev))
#             last_day_vol[tick.token] = int(tick.volume)

#         if bucket > 0:
#             prev_bucket = cur_bucket.get(tick.token)

#             if prev_bucket is None:
#                 cur_bucket[tick.token] = bucket
#                 cur_candle[tick.token] = {
#                     "open": float(tick.ltp),
#                     "high": float(tick.ltp),
#                     "low": float(tick.ltp),
#                     "close": float(tick.ltp),
#                     "volume": int(v_delta),
#                     "bucket_epoch": bucket,
#                 }
#             elif bucket == prev_bucket:
#                 c = cur_candle.get(tick.token)
#                 if c:
#                     c["high"] = max(float(c["high"]), float(tick.ltp))
#                     c["low"] = min(float(c["low"]), float(tick.ltp))
#                     c["close"] = float(tick.ltp)
#                     c["volume"] = int(c.get("volume", 0)) + int(v_delta)
#             else:
#                 closed = cur_candle.get(tick.token)
#                 if closed:
#                     lock = TOKEN_LOCKS.setdefault(tick.token, asyncio.Lock())
#                     async with lock:
#                         try:
#                             await BreakoutEngine.on_candle_close(int(tick.token), dict(closed), ENGINE_STATE)
#                         except Exception as e:
#                             logger.error(f"CandleClose error token={tick.token}: {e}")

#                 cur_bucket[tick.token] = bucket
#                 cur_candle[tick.token] = {
#                     "open": float(tick.ltp),
#                     "high": float(tick.ltp),
#                     "low": float(tick.ltp),
#                     "close": float(tick.ltp),
#                     "volume": int(v_delta),
#                     "bucket_epoch": bucket,
#                 }

#         # Tick-fast breakout
#         lock = TOKEN_LOCKS.setdefault(tick.token, asyncio.Lock())
#         async with lock:
#             try:
#                 await BreakoutEngine.run(
#                     token=int(tick.token),
#                     ltp=float(tick.ltp),
#                     vol=int(tick.volume or 0),
#                     state=ENGINE_STATE,
#                 )
#             except Exception as e:
#                 logger.error(f"Breakout run error token={tick.token}: {e}")

#         _sync_ui_orders_from_trades()


# # -----------------------------
# # FastAPI
# # -----------------------------
# app = FastAPI(title="Nexus Core", version="2.3")

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=False,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


# @app.get("/")
# async def root():
#     if not os.path.exists(INDEX_HTML):
#         return JSONResponse({"error": "index.html not found", "path": INDEX_HTML}, status_code=404)
#     return FileResponse(INDEX_HTML)


# @app.get("/api/stats")
# async def api_stats():
#     data_connected = {
#         "breakout": (STATE.engine_status.get("bull") == "1" or STATE.engine_status.get("bear") == "1"),
#         "momentum": (STATE.engine_status.get("mom_bull") == "1" or STATE.engine_status.get("mom_bear") == "1"),
#         "dhan": STATE.dhan_connected,
#         "order_updates": STATE.order_updates_connected,
#     }
#     return {
#         "redis_ok": STATE.redis_ok,
#         "dhan_connected": STATE.dhan_connected,
#         "dhan_last_msg_sec_ago": round(time.time() - (STATE.dhan_last_msg_ts or time.time()), 2),
#         "order_updates_connected": STATE.order_updates_connected,
#         "order_last_msg_sec_ago": round(time.time() - (STATE.order_last_msg_ts or time.time()), 2),
#         "engine_status": STATE.engine_status,
#         "data_connected": data_connected,
#         "pnl": STATE.pnl,
#         "dhan_client_ready": bool(ENGINE_STATE.get("dhan") is not None),
#     }


# @app.get("/api/orders")
# async def api_orders():
#     _sync_ui_orders_from_trades()
#     return STATE.orders


# @app.get("/api/scanner")
# async def api_scanner():
#     return STATE.scanner


# @app.get("/api/config/auth")
# async def api_config_auth():
#     client_id, access_token = await _get_dhan_creds()
#     return {"client_id": client_id or "", "access_token": access_token or ""}


# @app.post("/api/settings/engine/{side}")
# async def save_engine_settings(side: str, payload: Dict[str, Any]):
#     if side not in ENGINE_SIDES:
#         raise HTTPException(status_code=400, detail="Invalid side")

#     ok = await _save_engine_settings(side, payload)
#     if not ok:
#         raise HTTPException(status_code=500, detail="Failed to save settings")

#     if side in ("bull", "bear"):
#         ENGINE_STATE["config"][side] = payload or {}

#     return {"status": "success"}


# @app.get("/api/settings/engine/{side}")
# async def get_engine_settings(side: str):
#     if side not in ENGINE_SIDES:
#         raise HTTPException(status_code=400, detail="Invalid side")
#     return await _load_engine_settings(side)


# @app.post("/api/control")
# async def api_control(payload: Dict[str, Any]):
#     action = payload.get("action")

#     if action == "toggle_engine":
#         side = payload.get("side")
#         enabled = _parse_bool(payload.get("enabled", payload.get("enabled_str", payload.get("enabled_int"))))
#         if side not in ENGINE_SIDES:
#             raise HTTPException(status_code=400, detail="Invalid side")
#         await _save_engine_status(side, enabled)
#         return {"status": "success", "engine_status": STATE.engine_status}

#     if action == "save_api":
#         client_id = (payload.get("client_id") or "").strip()
#         access_token = (payload.get("access_token") or "").strip()
#         if not client_id or not access_token:
#             raise HTTPException(status_code=400, detail="client_id and access_token required")

#         save_fn = getattr(TradeControl, "save_config", None)
#         if callable(save_fn):
#             ok = await save_fn(client_id, access_token)
#             if not ok:
#                 raise HTTPException(status_code=500, detail="Failed to save credentials")
#         else:
#             r = await _get_redis_client()
#             await r.set("nexus:config:api_key", client_id)
#             await r.set("nexus:config:api_secret", access_token)
#             await r.set("nexus:auth:access_token", access_token)

#         # Re-init immediately
#         await _init_dhan_client_into_engine_state(force=True)
#         return {"status": "success", "dhan_client_ready": bool(ENGINE_STATE.get("dhan") is not None)}

#     if action == "manual_exit_one":
#         symbol = (payload.get("symbol") or "").strip().upper()
#         if symbol:
#             ENGINE_STATE["manual_exits"].add(symbol)
#         return {"status": "success"}

#     if action == "square_off_one":
#         side = payload.get("side")
#         symbol = payload.get("symbol")
#         if side in STATE.orders:
#             STATE.orders[side] = [o for o in STATE.orders[side] if o.get("symbol") != symbol]
#         return {"status": "success"}

#     if action == "square_off_all":
#         side = payload.get("side")
#         if side in STATE.orders:
#             STATE.orders[side] = []
#         return {"status": "success"}

#     raise HTTPException(status_code=400, detail="Unknown action")


# # -----------------------------
# # Startup helpers (engine init)
# # -----------------------------
# async def _init_engine_live_flags() -> None:
#     ENGINE_STATE["engine_live"]["bull"] = (STATE.engine_status.get("bull") == "1")
#     ENGINE_STATE["engine_live"]["bear"] = (STATE.engine_status.get("bear") == "1")


# async def _init_engine_configs() -> None:
#     ENGINE_STATE["config"]["bull"] = await _load_engine_settings("bull")
#     ENGINE_STATE["config"]["bear"] = await _load_engine_settings("bear")


# async def _init_engine_stocks() -> None:
#     stocks: Dict[int, Dict[str, Any]] = {}

#     for t in STATE.universe_tokens:
#         md = STATE.market_data.get(int(t), {}) or {}
#         sym = STATE.token_to_symbol.get(int(t), str(t))

#         stocks[int(t)] = {
#             "token": int(t),
#             "security_id": int(t),  # you use token==security_id
#             "symbol": sym,

#             "pdh": float(md.get("pdh") or 0.0),
#             "pdl": float(md.get("pdl") or 0.0),
#             "sma": float(md.get("sma") or 0.0),

#             "ltp": 0.0,
#             "brk_status": "WAITING",
#         }

#         TOKEN_LOCKS.setdefault(int(t), asyncio.Lock())

#     ENGINE_STATE["stocks"] = stocks


# # -----------------------------
# # Startup / shutdown
# # -----------------------------
# @app.on_event("startup")
# async def on_startup():
#     logger.info("System startup")

#     # Redis init
#     try:
#         r = await _get_redis_client()
#         await r.ping()
#         STATE.redis_ok = True
#         logger.info("Redis OK")
#     except Exception as e:
#         STATE.redis_ok = False
#         logger.error(f"Redis init failed: {e}")

#     # engine toggles
#     try:
#         await _load_engine_status()
#     except Exception:
#         pass

#     # universe
#     try:
#         tokens, symbols = await _load_universe_from_redis()
#         STATE.universe_tokens = tokens
#         STATE.universe_symbols = symbols
#         STATE.token_to_symbol = {int(t): symbols[i] for i, t in enumerate(tokens) if i < len(symbols)}
#         logger.info(f"Universe loaded. tokens={len(tokens)}")
#     except Exception as e:
#         logger.error(f"Universe load failed: {e}")

#     # market data cache
#     try:
#         if STATE.universe_tokens:
#             STATE.market_data = await _load_market_data_bulk(STATE.universe_tokens)
#             logger.info(f"Market data cached. tokens={len(STATE.market_data)}")
#     except Exception as e:
#         logger.warning(f"Market data cache load failed: {e}")

#     # init engine state
#     await _init_engine_live_flags()
#     await _init_engine_configs()
#     await _init_engine_stocks()

#     # init dhan client (might still be None if creds not saved yet; auto reinit will fix)
#     await _init_dhan_client_into_engine_state(force=True)

#     # tasks
#     STATE.stop_event.clear()
#     STATE.dispatch_task = asyncio.create_task(tick_dispatcher(), name="tick_dispatcher")
#     STATE.ws_task = asyncio.create_task(dhan_marketfeed_worker(), name="dhan_marketfeed_worker")
#     STATE.order_ws_task = asyncio.create_task(dhan_order_updates_worker(), name="dhan_order_updates_worker")

#     logger.info("Startup complete")


# @app.on_event("shutdown")
# async def on_shutdown():
#     logger.info("Shutdown requested")
#     STATE.stop_event.set()

#     for t in (STATE.ws_task, STATE.order_ws_task, STATE.dispatch_task):
#         if t and not t.done():
#             t.cancel()

#     for t in (STATE.ws_task, STATE.order_ws_task, STATE.dispatch_task):
#         if t:
#             try:
#                 await t
#             except Exception:
#                 pass

#     try:
#         if STATE._ws is not None:
#             await STATE._ws.send(json.dumps({"RequestCode": 12}))
#             await STATE._ws.close()
#     except Exception:
#         pass

#     try:
#         if STATE._order_ws is not None:
#             await STATE._order_ws.close()
#     except Exception:
#         pass

#     logger.info("Shutdown complete")
import os
import json
import time
import struct
import asyncio
import logging
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

# -----------------------------
# Optional fast json (orjson)
# -----------------------------
try:
    import orjson as _orjson  # type: ignore

    def _json_dumps(obj: Any) -> str:
        return _orjson.dumps(obj).decode("utf-8")

    def _json_loads(s: Any) -> Any:
        if isinstance(s, (bytes, bytearray, memoryview)):
            return _orjson.loads(bytes(s))
        return _orjson.loads(s)

except Exception:
    def _json_dumps(obj: Any) -> str:
        return json.dumps(obj)

    def _json_loads(s: Any) -> Any:
        return json.loads(s)


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

# Tick queues (token-int queues; coalesced tick payload stored in STATE.latest_ticks)
TICK_QUEUE_MAX = int(os.getenv("TICK_QUEUE_MAX", "20000"))

# Dispatcher workers (token partitioning removes per-token locks)
DISPATCHER_WORKERS = int(os.getenv("DISPATCHER_WORKERS", "4"))

# UI engine sides
ENGINE_SIDES = ["bull", "mom_bull", "bear", "mom_bear"]

# Candle TF minutes (BreakoutEngine needs candle closes)
CANDLE_TF_MINUTES = int(os.getenv("CANDLE_TF_MINUTES", "1"))

# WS keepalive (recommended on Heroku)
WS_PING_INTERVAL = float(os.getenv("WS_PING_INTERVAL", "20"))
WS_PING_TIMEOUT = float(os.getenv("WS_PING_TIMEOUT", "20"))

# Auto reinit dhan client every N seconds if missing (startup race / creds saved later)
DHAN_REINIT_INTERVAL_SEC = int(os.getenv("DHAN_REINIT_INTERVAL_SEC", "10"))

# UI sync throttling
UI_SYNC_INTERVAL_SEC = float(os.getenv("UI_SYNC_INTERVAL_SEC", "0.2"))

# Scanner throttling (avoid inserting same signal too frequently)
SCANNER_THROTTLE_SEC = float(os.getenv("SCANNER_THROTTLE_SEC", "1.0"))


# -----------------------------
# Binary parsing (Dhan WS is little-endian)
# Header: 8 bytes -> code(1), length(2), exch(1), secid(4)
# -----------------------------
HDR_STRUCT = struct.Struct("<BHBI")  # 8 bytes
TICKER_STRUCT = struct.Struct("<fI")
PREVCLOSE_STRUCT = struct.Struct("<fI")
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

        # UI placeholders
        self.pnl: Dict[str, float] = {s: 0.0 for s in ENGINE_SIDES}
        self.pnl["total"] = 0.0
        self.orders: Dict[str, List[Dict[str, Any]]] = {s: [] for s in ENGINE_SIDES}
        self.scanner: Dict[str, List[Dict[str, Any]]] = {s: [] for s in ENGINE_SIDES}

        # Cached market data (pdh/pdl/sma/etc)
        self.market_data: Dict[int, Dict[str, Any]] = {}

        # asyncio
        self.stop_event = asyncio.Event()

        # Coalesced tick store: token -> (ltp, ltt_epoch, vol, day_open, day_high, day_low)
        self.latest_ticks: Dict[int, Tuple[float, int, int, float, float, float]] = {}

        # Pending token markers to avoid enqueueing same token repeatedly
        self._pending_tokens: set[int] = set()

        # Dispatcher queues: token ints (partitioned)
        self.dispatcher_workers: int = max(1, int(DISPATCHER_WORKERS))
        self.tick_queues: List[asyncio.Queue[int]] = [
            asyncio.Queue(maxsize=max(1, int(TICK_QUEUE_MAX // max(1, self.dispatcher_workers))))
            for _ in range(self.dispatcher_workers)
        ]

        self.ws_task: Optional[asyncio.Task] = None
        self.order_ws_task: Optional[asyncio.Task] = None
        self.dispatch_tasks: List[asyncio.Task] = []
        self.dhan_reinit_task: Optional[asyncio.Task] = None

        # Backwards-compatible single ref (first worker)
        self.dispatch_task: Optional[asyncio.Task] = None

        # WS refs for shutdown
        self._ws: Optional[WebSocketClientProtocol] = None
        self._order_ws: Optional[WebSocketClientProtocol] = None

        # dhan init tracking
        self._dhan_init_ts: float = 0.0
        self._dhan_last_client_id: str = ""
        self._dhan_last_token: str = ""

        # UI sync throttle
        self._last_ui_sync_ts: float = 0.0


STATE = RuntimeState()


# -----------------------------
# Engine state passed to BreakoutEngine
# -----------------------------
ENGINE_STATE: Dict[str, Any] = {
    "stocks": {},          # token -> stock dict
    "config": {},          # side -> config dict
    "engine_live": {},     # side -> bool (MUST be bool)
    "trades": {"bull": [], "bear": []},
    "manual_exits": set(),
    "dhan": None,          # dhanhq client instance
}


# -----------------------------
# Redis / creds helpers
# -----------------------------
async def _get_redis_client():
    return await get_redis()


async def _get_dhan_creds() -> Tuple[str, str]:
    client_id = ""
    access_token = ""

    try:
        cid, _ = await TradeControl.get_config()
        tok = await TradeControl.get_access_token()
        client_id = (cid or "").strip()
        access_token = (tok or "").strip()
    except Exception:
        pass

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
        if not access_token:
            try:
                v = await r.get("nexus:config:api_secret")
                access_token = (v or "").strip()
            except Exception:
                pass

    return client_id, access_token


async def _init_dhan_client_into_engine_state(force: bool = False) -> None:
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

    if (STATE._dhan_last_client_id != client_id) or (STATE._dhan_last_token != access_token):
        STATE._dhan_last_client_id = client_id
        STATE._dhan_last_token = access_token
        ENGINE_STATE["dhan"] = None

    try:
        from dhanhq import dhanhq
        ENGINE_STATE["dhan"] = dhanhq(str(client_id), str(access_token))
        logger.info("✅ Dhan client initialized for order placement.")
    except Exception as e:
        ENGINE_STATE["dhan"] = None
        logger.error(f"❌ Dhan SDK init failed: {e}")


async def dhan_client_reinit_loop() -> None:
    """
    Keeps dhan client ready without touching the tick hot-path.
    """
    while not STATE.stop_event.is_set():
        try:
            await _init_dhan_client_into_engine_state(force=False)
        except Exception:
            pass
        await asyncio.sleep(max(1.0, float(DHAN_REINIT_INTERVAL_SEC)))


async def _load_universe_from_redis() -> Tuple[List[int], List[str]]:
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
    r = await _get_redis_client()
    for side in ENGINE_SIDES:
        v = await r.get(f"nexus:engine:{side}:enabled")
        if v is not None:
            STATE.engine_status[side] = "1" if str(v).strip().lower() in ("1", "true", "yes", "on") else "0"


async def _save_engine_status(side: str, enabled: bool) -> None:
    r = await _get_redis_client()
    await r.set(f"nexus:engine:{side}:enabled", "1" if enabled else "0")
    STATE.engine_status[side] = "1" if enabled else "0"

    if side in ("bull", "bear"):
        ENGINE_STATE["engine_live"][side] = bool(enabled)


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


def _parse_binary_message(data: bytes) -> Optional[Tuple[int, float, int, int, float, float, float]]:
    """
    Returns a compact tuple for hot-path:
      (token, ltp, ltt_epoch, volume, day_open, day_high, day_low)

    - volume/day_* may be 0 if not present
    """
    if not data or len(data) < 8:
        return None
    try:
        feed_code, msg_len, exch, sec_id = HDR_STRUCT.unpack_from(data, 0)
    except Exception:
        return None

    token = int(sec_id)

    # code 2: ticker
    if feed_code == 2 and len(data) >= 8 + TICKER_STRUCT.size:
        ltp, ltt = TICKER_STRUCT.unpack_from(data, 8)
        return (token, float(ltp), int(ltt), 0, 0.0, 0.0, 0.0)

    # code 4: quote
    if feed_code == 4 and len(data) >= 8 + QUOTE_STRUCT.size:
        (
            ltp, ltq, ltt, atp, vol, tsq, tbq,
            day_open, day_close, day_high, day_low
        ) = QUOTE_STRUCT.unpack_from(data, 8)
        return (token, float(ltp), int(ltt), int(vol), float(day_open), float(day_high), float(day_low))

    # code 6: prev_close
    if feed_code == 6 and len(data) >= 8 + PREVCLOSE_STRUCT.size:
        prev_close, oi = PREVCLOSE_STRUCT.unpack_from(data, 8)
        md = STATE.market_data.get(token) or {}
        md["prev_close"] = float(prev_close)
        STATE.market_data[token] = md
        return None

    # code 50: disconnect
    if feed_code == 50:
        STATE.dhan_connected = False
        return None

    return None


async def dhan_marketfeed_worker() -> None:
    backoff = 1.0
    max_backoff = 20.0

    workers = max(1, STATE.dispatcher_workers)
    queues = STATE.tick_queues

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

                while not STATE.stop_event.is_set():
                    msg = await ws.recv()
                    STATE.dhan_last_msg_ts = time.time()

                    if not isinstance(msg, (bytes, bytearray, memoryview)):
                        continue

                    parsed = _parse_binary_message(bytes(msg))
                    if not parsed:
                        continue

                    token, ltp, ltt_epoch, vol, day_open, day_high, day_low = parsed

                    # update coalesced tick store
                    STATE.latest_ticks[token] = (ltp, ltt_epoch, vol, day_open, day_high, day_low)

                    # enqueue token only if not pending (coalesce)
                    if token not in STATE._pending_tokens:
                        q = queues[token % workers]
                        try:
                            STATE._pending_tokens.add(token)
                            q.put_nowait(token)
                        except asyncio.QueueFull:
                            # if we couldn't enqueue, allow future enqueue attempts
                            STATE._pending_tokens.discard(token)

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
        side = str(trade.get("side") or "").lower()

        if side == "bull":
            trade["target_price"] = round(ep + (init_risk * rr_val), 2)
        elif side == "bear":
            trade["target_price"] = round(ep - (init_risk * rr_val), 2)

        trade["trail_step"] = float(init_risk * tsl_ratio) if tsl_ratio > 0 else float(init_risk)
    except Exception:
        return


def _sync_ui_orders_from_trades() -> None:
    try:
        STATE.orders["bull"] = list(ENGINE_STATE["trades"].get("bull", []))
        STATE.orders["bear"] = list(ENGINE_STATE["trades"].get("bear", []))
    except Exception:
        pass


async def dhan_order_updates_worker() -> None:
    backoff = 1.0
    max_backoff = 20.0

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
                        for side in ("bull", "bear"):
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

                    # UI sync is cheap here (order updates rate is low vs ticks)
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
# Dispatcher + Scanner + Candle builder + BreakoutEngine
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


def _bucket_epoch(epoch: int, tf_min: int) -> int:
    if epoch <= 0:
        return 0
    tf_sec = int(tf_min) * 60
    return int(epoch // tf_sec) * tf_sec


def _maybe_sync_ui_orders() -> None:
    now = time.time()
    if (now - (STATE._last_ui_sync_ts or 0.0)) >= float(UI_SYNC_INTERVAL_SEC):
        STATE._last_ui_sync_ts = now
        _sync_ui_orders_from_trades()


async def tick_dispatcher_worker(worker_id: int) -> None:
    """
    Each worker owns a partition of tokens: token % workers == worker_id.
    This removes the need for per-token locks.
    """
    logger.info(f"Tick dispatcher worker {worker_id} active.")

    q = STATE.tick_queues[worker_id]
    cur_bucket: Dict[int, int] = {}
    cur_candle: Dict[int, Dict[str, Any]] = {}
    last_day_vol: Dict[int, int] = {}
    last_scanner_ts: Dict[Tuple[str, int], float] = {}

    while not STATE.stop_event.is_set():
        try:
            token = await q.get()
        except asyncio.CancelledError:
            break

        # mark token as not pending now (it can be re-enqueued)
        STATE._pending_tokens.discard(token)

        tick = STATE.latest_ticks.get(token)
        if not tick:
            continue

        ltp, ltt_epoch, vol, day_open, day_high, day_low = tick
        vol = int(vol or 0)
        ltt_epoch = int(ltt_epoch or 0)

        # Scanner PDH/PDL
        md = STATE.market_data.get(token) or {}
        pdh = float(md.get("pdh") or 0.0)
        pdl = float(md.get("pdl") or 0.0)

        now_ts = time.time()
        if pdh > 0.0 and ltp >= pdh:
            key = ("bull", token)
            if (now_ts - last_scanner_ts.get(key, 0.0)) >= float(SCANNER_THROTTLE_SEC):
                last_scanner_ts[key] = now_ts
                sym = STATE.token_to_symbol.get(token, str(token))
                _push_scanner_signal("bull", sym, float(ltp), "LTP >= PDH")

        if pdl > 0.0 and ltp <= pdl:
            key = ("bear", token)
            if (now_ts - last_scanner_ts.get(key, 0.0)) >= float(SCANNER_THROTTLE_SEC):
                last_scanner_ts[key] = now_ts
                sym = STATE.token_to_symbol.get(token, str(token))
                _push_scanner_signal("bear", sym, float(ltp), "LTP <= PDL")

        # Candle builder
        bucket = _bucket_epoch(ltt_epoch, CANDLE_TF_MINUTES)

        v_delta = 0
        if vol > 0:
            prev = last_day_vol.get(token)
            if prev is not None:
                v_delta = max(0, vol - int(prev))
            last_day_vol[token] = vol

        if bucket > 0:
            prev_bucket = cur_bucket.get(token)
            if prev_bucket is None:
                cur_bucket[token] = bucket
                cur_candle[token] = {
                    "open": float(ltp),
                    "high": float(ltp),
                    "low": float(ltp),
                    "close": float(ltp),
                    "volume": int(v_delta),
                    "bucket_epoch": bucket,
                }
            elif bucket == prev_bucket:
                c = cur_candle.get(token)
                if c:
                    # micro-optimized updates
                    h = float(c["high"])
                    lo = float(c["low"])
                    px = float(ltp)
                    if px > h:
                        c["high"] = px
                    if px < lo:
                        c["low"] = px
                    c["close"] = px
                    c["volume"] = int(c.get("volume", 0)) + int(v_delta)
            else:
                closed = cur_candle.get(token)
                if closed:
                    try:
                        # close candle -> precompute trigger watch etc inside engine
                        await BreakoutEngine.on_candle_close(int(token), dict(closed), ENGINE_STATE)
                    except Exception as e:
                        logger.error(f"CandleClose error token={token}: {e}")

                cur_bucket[token] = bucket
                cur_candle[token] = {
                    "open": float(ltp),
                    "high": float(ltp),
                    "low": float(ltp),
                    "close": float(ltp),
                    "volume": int(v_delta),
                    "bucket_epoch": bucket,
                }

        # Tick-fast breakout (NO per-tick locks; token partitioned)
        try:
            await BreakoutEngine.run(
                token=int(token),
                ltp=float(ltp),
                vol=int(vol),
                state=ENGINE_STATE,
            )
        except Exception as e:
            logger.error(f"Breakout run error token={token}: {e}")

        _maybe_sync_ui_orders()


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
        "dhan_client_ready": bool(ENGINE_STATE.get("dhan") is not None),
    }


@app.get("/api/orders")
async def api_orders():
    _sync_ui_orders_from_trades()
    return STATE.orders


@app.get("/api/scanner")
async def api_scanner():
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

    if side in ("bull", "bear"):
        ENGINE_STATE["config"][side] = payload or {}

    return {"status": "success"}


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

        await _init_dhan_client_into_engine_state(force=True)
        return {"status": "success", "dhan_client_ready": bool(ENGINE_STATE.get("dhan") is not None)}

    if action == "manual_exit_one":
        symbol = (payload.get("symbol") or "").strip().upper()
        if symbol:
            ENGINE_STATE["manual_exits"].add(symbol)
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
# Startup helpers (engine init)
# -----------------------------
async def _init_engine_live_flags() -> None:
    ENGINE_STATE["engine_live"]["bull"] = (STATE.engine_status.get("bull") == "1")
    ENGINE_STATE["engine_live"]["bear"] = (STATE.engine_status.get("bear") == "1")


async def _init_engine_configs() -> None:
    ENGINE_STATE["config"]["bull"] = await _load_engine_settings("bull")
    ENGINE_STATE["config"]["bear"] = await _load_engine_settings("bear")


async def _init_engine_stocks() -> None:
    stocks: Dict[int, Dict[str, Any]] = {}

    for t in STATE.universe_tokens:
        md = STATE.market_data.get(int(t), {}) or {}
        sym = STATE.token_to_symbol.get(int(t), str(t))

        stocks[int(t)] = {
            "token": int(t),
            "security_id": int(t),
            "symbol": sym,

            "pdh": float(md.get("pdh") or 0.0),
            "pdl": float(md.get("pdl") or 0.0),
            "sma": float(md.get("sma") or 0.0),

            "ltp": 0.0,
            "brk_status": "WAITING",
        }

    ENGINE_STATE["stocks"] = stocks


# -----------------------------
# Startup / shutdown
# -----------------------------
@app.on_event("startup")
async def on_startup():
    logger.info("System startup")

    try:
        r = await _get_redis_client()
        await r.ping()
        STATE.redis_ok = True
        logger.info("Redis OK")
    except Exception as e:
        STATE.redis_ok = False
        logger.error(f"Redis init failed: {e}")

    try:
        await _load_engine_status()
    except Exception:
        pass

    try:
        tokens, symbols = await _load_universe_from_redis()
        STATE.universe_tokens = tokens
        STATE.universe_symbols = symbols
        STATE.token_to_symbol = {int(t): symbols[i] for i, t in enumerate(tokens) if i < len(symbols)}
        logger.info(f"Universe loaded. tokens={len(tokens)}")
    except Exception as e:
        logger.error(f"Universe load failed: {e}")

    try:
        if STATE.universe_tokens:
            STATE.market_data = await _load_market_data_bulk(STATE.universe_tokens)
            logger.info(f"Market data cached. tokens={len(STATE.market_data)}")
    except Exception as e:
        logger.warning(f"Market data cache load failed: {e}")

    await _init_engine_live_flags()
    await _init_engine_configs()
    await _init_engine_stocks()

    await _init_dhan_client_into_engine_state(force=True)

    STATE.stop_event.clear()

    # Start dispatcher workers
    STATE.dispatch_tasks = [
        asyncio.create_task(tick_dispatcher_worker(i), name=f"tick_dispatcher_{i}")
        for i in range(max(1, STATE.dispatcher_workers))
    ]
    STATE.dispatch_task = STATE.dispatch_tasks[0] if STATE.dispatch_tasks else None

    # Marketfeed + order updates
    STATE.ws_task = asyncio.create_task(dhan_marketfeed_worker(), name="dhan_marketfeed_worker")
    STATE.order_ws_task = asyncio.create_task(dhan_order_updates_worker(), name="dhan_order_updates_worker")

    # Periodic dhan client re-init (keeps hot path clean)
    STATE.dhan_reinit_task = asyncio.create_task(dhan_client_reinit_loop(), name="dhan_client_reinit_loop")

    logger.info("Startup complete")


@app.on_event("shutdown")
async def on_shutdown():
    logger.info("Shutdown requested")
    STATE.stop_event.set()

    # cancel tasks
    tasks: List[asyncio.Task] = []
    if STATE.ws_task:
        tasks.append(STATE.ws_task)
    if STATE.order_ws_task:
        tasks.append(STATE.order_ws_task)
    tasks.extend(STATE.dispatch_tasks or [])
    if STATE.dhan_reinit_task:
        tasks.append(STATE.dhan_reinit_task)

    for t in tasks:
        if t and not t.done():
            t.cancel()

    for t in tasks:
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
