# # # -*- coding: utf-8 -*-
# # import asyncio
# # import os
# # import logging
# # import time
# # import threading
# # from datetime import datetime
# # from typing import Dict, Any, Optional, List
# # from collections import defaultdict

# # import pytz
# # from fastapi import FastAPI, Request
# # from fastapi.responses import HTMLResponse, FileResponse
# # from fastapi.middleware.cors import CORSMiddleware

# # # --- DhanHQ SDK (supports v2.1.0+; includes safe fallback for older layouts)
# # try:
# #     # v2.1.0+ (recommended)
# #     from dhanhq import DhanContext, dhanhq, MarketFeed, OrderUpdate
# # except Exception:  # pragma: no cover
# #     # Older package layouts (best-effort compatibility)
# #     from dhanhq import dhanhq
# #     try:
# #         from dhanhq import DhanContext  # type: ignore
# #     except Exception:
# #         DhanContext = None  # type: ignore
# #     try:
# #         from dhanhq import MarketFeed  # type: ignore
# #     except Exception:
# #         MarketFeed = None  # type: ignore
# #     try:
# #         from dhanhq import OrderUpdate  # type: ignore
# #     except Exception:
# #         OrderUpdate = None  # type: ignore

# # from redis_manager import TradeControl
# # from breakout_engine import BreakoutEngine
# # from momentum_engine import MomentumEngine

# # # -----------------------------
# # # LOGGING / TIMEZONE
# # # -----------------------------
# # logging.basicConfig(
# #     level=logging.INFO,
# #     format="%(asctime)s [MAIN] %(message)s",
# #     datefmt="%Y-%m-%d %H:%M:%S",
# # )
# # logger = logging.getLogger("Nexus_Main")
# # IST = pytz.timezone("Asia/Kolkata")

# # # -----------------------------
# # # FASTAPI APP
# # # -----------------------------
# # app = FastAPI(title="Nexus Core", version="2.7.0", strict_slashes=False)

# # app.add_middleware(
# #     CORSMiddleware,
# #     allow_origins=["*"],
# #     allow_credentials=True,
# #     allow_methods=["*"],
# #     allow_headers=["*"],
# # )

# # # -----------------------------
# # # DEFAULTS
# # # -----------------------------
# # def _default_volume_matrix() -> list:
# #     multipliers = [20, 18, 16, 14, 12, 10, 8, 6, 4, 2]
# #     out = []
# #     for i in range(10):
# #         out.append({
# #             "min_vol_price_cr": float(i + 1),
# #             "sma_multiplier": float(multipliers[i]),
# #             "min_sma_avg": int(1000),
# #         })
# #     return out


# # DEFAULT_CONFIG: Dict[str, Dict[str, Any]] = {
# #     "bull": {"risk_reward": "1:2", "trailing_sl": "1:1.5", "total_trades": 5, "risk_trade_1": 2000,
# #              "volume_criteria": _default_volume_matrix(), "trade_start": "09:15", "trade_end": "15:10"},
# #     "bear": {"risk_reward": "1:2", "trailing_sl": "1:1.5", "total_trades": 5, "risk_trade_1": 2000,
# #              "volume_criteria": _default_volume_matrix(), "trade_start": "09:15", "trade_end": "15:10"},
# #     "mom_bull": {"risk_reward": "1:2", "trailing_sl": "1:1.5", "total_trades": 5, "risk_trade_1": 2000,
# #                  "volume_criteria": _default_volume_matrix(), "trade_start": "09:15", "trade_end": "09:17"},
# #     "mom_bear": {"risk_reward": "1:2", "trailing_sl": "1:1.5", "total_trades": 5, "risk_trade_1": 2000,
# #                  "volume_criteria": _default_volume_matrix(), "trade_start": "09:15", "trade_end": "09:17"},
# # }

# # # -----------------------------
# # # GLOBAL RAM STATE
# # # -----------------------------
# # RAM_STATE: Dict[str, Any] = {
# #     "main_loop": None,
# #     "tick_queue": None,

# #     # Dhan session
# #     "dhan_context": None,
# #     "dhan": None,
# #     "dhan_client_id": "",
# #     "dhan_access_token": "",

# #     # Dhan WS clients + threads
# #     "marketfeed": None,
# #     "orderupdate": None,
# #     "marketfeed_thread": None,
# #     "orderupdate_thread": None,

# #     # Universe
# #     "stocks": {},  # key = security_id (int)

# #     # Trades & toggles
# #     "trades": {"bull": [], "bear": [], "mom_bull": [], "mom_bear": []},
# #     "engine_live": {"bull": True, "bear": True, "mom_bull": True, "mom_bear": True},
# #     "config": {k: dict(v) for k, v in DEFAULT_CONFIG.items()},
# #     "manual_exits": set(),

# #     # Status
# #     "data_connected": {"breakout": False, "momentum": False},
# #     "order_updates_connected": False,

# #     # Parallel Processing Infrastructure
# #     "token_locks": defaultdict(asyncio.Lock),
# #     "engine_sem": None,
# #     "inflight": set(),
# #     "max_inflight": 5000,
# #     "candle_close_queue": None,
# #     "tick_batches_dropped": 0,
# #     "tick_batches_enqueued": 0,
# # }

# # # -----------------------------
# # # HELPERS
# # # -----------------------------
# # def _now_ist() -> datetime:
# #     return datetime.now(IST)

# # def _compute_pnl() -> Dict[str, float]:
# #     pnl = {k: 0.0 for k in ["bull", "bear", "mom_bull", "mom_bear"]}
# #     for side in pnl:
# #         pnl[side] = float(sum(float(t.get("pnl", 0.0) or 0.0) for t in RAM_STATE["trades"].get(side, [])))
# #     pnl["total"] = float(sum(pnl[s] for s in ["bull", "bear", "mom_bull", "mom_bear"]))
# #     return pnl

# # def _trade_is_open(t: dict) -> bool:
# #     return str(t.get("status", "OPEN")).upper() == "OPEN"

# # def _chunked(arr: List[Any], n: int) -> List[List[Any]]:
# #     out = []
# #     for i in range(0, len(arr), n):
# #         out.append(arr[i:i+n])
# #     return out

# # # -----------------------------
# # # CENTRALIZED 1m CANDLE AGGREGATOR
# # # -----------------------------
# # def _update_1m_candle(stock: dict, ltp: float, cum_vol: int) -> Optional[dict]:
# #     now = _now_ist()
# #     bucket = now.replace(second=0, microsecond=0)
# #     c = stock.get("candle_1m")
# #     last_cum = int(stock.get("candle_last_cum_vol", 0) or 0)

# #     if not c:
# #         stock["candle_1m"] = {"bucket": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": 0}
# #         stock["candle_last_cum_vol"] = cum_vol
# #         return None

# #     if c["bucket"] != bucket:
# #         closed = dict(c)
# #         stock["candle_1m"] = {"bucket": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": 0}
# #         stock["candle_last_cum_vol"] = cum_vol
# #         return closed

# #     c["high"] = max(c["high"], ltp)
# #     c["low"] = min(c["low"], ltp)
# #     c["close"] = ltp
# #     if last_cum > 0:
# #         c["volume"] += max(0, cum_vol - last_cum)
# #     stock["candle_last_cum_vol"] = cum_vol
# #     return None

# # # -----------------------------
# # # PARALLEL TICK PIPELINE
# # # -----------------------------
# # async def _process_tick_task(token: int, ltp: float, vol: int):
# #     sem = RAM_STATE["engine_sem"]
# #     lock = RAM_STATE["token_locks"][token]
# #     async with sem:
# #         async with lock:
# #             stock = RAM_STATE["stocks"].get(token)
# #             if not stock:
# #                 return

# #             stock["ltp"] = float(ltp or 0.0)
# #             stock["last_update_ts"] = time.time()

# #             closed = _update_1m_candle(stock, float(ltp or 0.0), int(vol or 0))
# #             if closed:
# #                 RAM_STATE["candle_close_queue"].put_nowait((token, closed))

# #             await asyncio.gather(
# #                 BreakoutEngine.run(token, float(ltp or 0.0), int(vol or 0), RAM_STATE),
# #                 MomentumEngine.run(token, float(ltp or 0.0), int(vol or 0), RAM_STATE),
# #                 return_exceptions=True
# #             )

# # async def tick_worker_parallel():
# #     q = RAM_STATE["tick_queue"]
# #     inflight = RAM_STATE["inflight"]
# #     logger.info("🧵 Parallel Tick Worker: Active")
# #     while True:
# #         ticks = await q.get()
# #         try:
# #             if len(inflight) >= RAM_STATE["max_inflight"]:
# #                 done, _ = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
# #                 inflight.difference_update(done)

# #             for tick in ticks:
# #                 token = tick.get("instrument_token")
# #                 if not token:
# #                     continue
# #                 task = asyncio.create_task(
# #                     _process_tick_task(
# #                         int(token),
# #                         float(tick.get("last_price", 0) or 0),
# #                         int(tick.get("volume_traded", 0) or 0),
# #                     )
# #                 )
# #                 inflight.add(task)
# #                 task.add_done_callback(inflight.discard)
# #         finally:
# #             q.task_done()

# # async def candle_worker():
# #     cq = RAM_STATE["candle_close_queue"]
# #     logger.info("🕯️ Background Candle Worker: Active")
# #     while True:
# #         token, candle = await cq.get()
# #         try:
# #             async with RAM_STATE["token_locks"][token]:
# #                 await asyncio.gather(
# #                     BreakoutEngine.on_candle_close(token, candle, RAM_STATE),
# #                     MomentumEngine.on_candle_close(token, candle, RAM_STATE),
# #                     return_exceptions=True
# #                 )
# #         finally:
# #             cq.task_done()

# # # -----------------------------
# # # DHAN: Normalize MarketFeed packets to internal tick schema
# # # -----------------------------
# # def _dhan_to_internal_tick(pkt: Any) -> Optional[dict]:
# #     """
# #     Returns:
# #       {"instrument_token": <security_id:int>, "last_price": <float>, "volume_traded": <int>}
# #     """
# #     if not pkt or not isinstance(pkt, dict):
# #         return None

# #     # security id
# #     sec = pkt.get("security_id") or pkt.get("SecurityId") or pkt.get("securityId") or pkt.get("SecId")
# #     try:
# #         sec_id = int(sec)
# #     except Exception:
# #         return None

# #     # ltp
# #     ltp = pkt.get("ltp") or pkt.get("Ltp") or pkt.get("LTP") or pkt.get("last_price") or pkt.get("LastTradedPrice")
# #     try:
# #         ltp_f = float(ltp)
# #     except Exception:
# #         ltp_f = 0.0

# #     # volume (Full/Quote packets include Volume; treat as cumulative volume)
# #     vol = pkt.get("volume") or pkt.get("Volume") or pkt.get("VTT") or pkt.get("volume_traded") or pkt.get("VolumeTraded")
# #     try:
# #         vol_i = int(vol)
# #     except Exception:
# #         vol_i = 0

# #     return {"instrument_token": sec_id, "last_price": ltp_f, "volume_traded": vol_i}

# # # -----------------------------
# # # DHAN: Order Update -> executed-price based recalculation (entry/exit)
# # # -----------------------------
# # async def _handle_order_update_async(order_msg: dict):
# #     """
# #     OrderUpdate stream gives JSON with "Data" containing fields like:
# #       OrderNo / OrderId, SecurityId, AvgTradedPrice, TradedQty, Status
# #     """
# #     data = (order_msg or {}).get("Data") or {}
# #     if not isinstance(data, dict):
# #         return

# #     order_no = str(
# #         data.get("OrderNo") or data.get("orderNo") or data.get("orderId") or data.get("OrderId") or ""
# #     ).strip()
# #     if not order_no:
# #         return

# #     sec = data.get("SecurityId") or data.get("securityId") or data.get("security_id")
# #     try:
# #         sec_id = int(sec)
# #     except Exception:
# #         sec_id = None

# #     avg_px = float(data.get("AvgTradedPrice") or 0.0)
# #     traded_qty = int(data.get("TradedQty") or 0)
# #     status = str(data.get("Status") or "").upper()

# #     if sec_id is not None:
# #         async with RAM_STATE["token_locks"][sec_id]:
# #             _apply_update_to_trades(order_no, avg_px, traded_qty, status)
# #     else:
# #         _apply_update_to_trades(order_no, avg_px, traded_qty, status)

# # def _apply_update_to_trades(order_no: str, avg_px: float, traded_qty: int, status: str):
# #     # Update entry fills + recompute target/trail based on executed price
# #     for side, arr in RAM_STATE["trades"].items():
# #         for t in arr:
# #             if str(t.get("order_id") or "") == order_no:
# #                 if avg_px > 0:
# #                     t["entry_exec_price"] = float(avg_px)
# #                 t["entry_traded_qty"] = int(traded_qty)
# #                 t["entry_order_status"] = status

# #                 # recompute once we have executed price
# #                 if avg_px > 0 and float(t.get("sl_price", 0) or 0) > 0:
# #                     entry_exec = float(avg_px)
# #                     sl = float(t.get("sl_price", 0) or 0)
# #                     rr_val = float(t.get("rr_val", 2.0) or 2.0)
# #                     tsl_ratio = float(t.get("tsl_ratio", 1.5) or 1.5)

# #                     risk_per_share = abs(entry_exec - sl)
# #                     if risk_per_share > 0:
# #                         is_bull = (str(t.get("side")) in ("bull", "mom_bull"))
# #                         t["entry_price"] = entry_exec  # executed entry becomes the truth
# #                         if is_bull:
# #                             t["target_price"] = round(entry_exec + (risk_per_share * rr_val), 2)
# #                         else:
# #                             t["target_price"] = round(entry_exec - (risk_per_share * rr_val), 2)

# #                         t["init_risk"] = float(risk_per_share)
# #                         t["trail_step"] = float(risk_per_share * tsl_ratio)

# #                 return  # order_no unique

# #             if str(t.get("exit_order_id") or "") == order_no:
# #                 if avg_px > 0:
# #                     t["exit_exec_price"] = float(avg_px)
# #                 t["exit_traded_qty"] = int(traded_qty)
# #                 t["exit_order_status"] = status
# #                 return

# # def _order_update_callback(order_msg: dict):
# #     loop = RAM_STATE.get("main_loop")
# #     if not loop:
# #         return
# #     try:
# #         asyncio.run_coroutine_threadsafe(_handle_order_update_async(order_msg), loop)
# #     except Exception:
# #         pass

# # # -----------------------------
# # # DHAN: MarketFeed thread
# # # -----------------------------
# # def _marketfeed_thread_fn():
# #     loop = RAM_STATE.get("main_loop")
# #     if not loop:
# #         return

# #     while True:
# #         try:
# #             ctx = RAM_STATE.get("dhan_context")
# #             if not ctx:
# #                 time.sleep(2)
# #                 continue

# #             instruments = []
# #             for sec_id, _s in RAM_STATE["stocks"].items():
# #                 instruments.append((MarketFeed.NSE, str(sec_id), MarketFeed.Full))

# #             if not instruments:
# #                 time.sleep(2)
# #                 continue

# #             batches = _chunked(instruments, 100)

# #             data = MarketFeed(ctx, batches[0], "v2")
# #             RAM_STATE["marketfeed"] = data

# #             for b in batches[1:]:
# #                 try:
# #                     data.subscribe_symbols(b)
# #                 except Exception:
# #                     pass

# #             RAM_STATE["data_connected"] = {"breakout": True, "momentum": True}
# #             logger.info(f"📡 DHAN MarketFeed Connected: subscribed {len(instruments)} security_ids")

# #             while True:
# #                 data.run_forever()
# #                 pkt = data.get_data()
# #                 tick = _dhan_to_internal_tick(pkt)
# #                 if not tick:
# #                     continue

# #                 def _put():
# #                     q = RAM_STATE.get("tick_queue")
# #                     if not q:
# #                         return
# #                     try:
# #                         q.put_nowait([tick])
# #                         RAM_STATE["tick_batches_enqueued"] += 1
# #                     except asyncio.QueueFull:
# #                         try:
# #                             q.get_nowait()
# #                             q.put_nowait([tick])
# #                             RAM_STATE["tick_batches_dropped"] += 1
# #                         except Exception:
# #                             pass

# #                 loop.call_soon_threadsafe(_put)

# #         except Exception as e:
# #             RAM_STATE["data_connected"] = {"breakout": False, "momentum": False}
# #             logger.error(f"❌ DHAN MarketFeed error: {e}")
# #             try:
# #                 mf = RAM_STATE.get("marketfeed")
# #                 if mf:
# #                     mf.disconnect()
# #             except Exception:
# #                 pass
# #             time.sleep(3)

# # # -----------------------------
# # # DHAN: OrderUpdate thread
# # # -----------------------------
# # def _orderupdate_thread_fn():
# #     while True:
# #         try:
# #             ctx = RAM_STATE.get("dhan_context")
# #             if not ctx:
# #                 time.sleep(2)
# #                 continue

# #             ou = OrderUpdate(ctx)
# #             ou.on_update = _order_update_callback
# #             RAM_STATE["orderupdate"] = ou

# #             RAM_STATE["order_updates_connected"] = True
# #             logger.info("🛰️ DHAN OrderUpdate Connected")

# #             ou.connect_to_dhan_websocket_sync()

# #         except Exception as e:
# #             RAM_STATE["order_updates_connected"] = False
# #             logger.error(f"❌ DHAN OrderUpdate error: {e}")
# #             time.sleep(3)

# # # -----------------------------
# # # FASTAPI ROUTES
# # # -----------------------------
# # @app.get("/", response_class=HTMLResponse)
# # async def home():
# #     try:
# #         return FileResponse("index.html")
# #     except Exception:
# #         return HTMLResponse("<h3>Dashboard file missing</h3>")

# # @app.get("/login")
# # @app.get("/login/")
# # async def login():
# #     return HTMLResponse(
# #         "<h3>Dhan Setup</h3>"
# #         "<p>Set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN as env vars, or use Dashboard Save API.</p>"
# #         "<p>Then restart app to connect MarketFeed + OrderUpdate.</p>"
# #     )

# # @app.get("/api/config/auth")
# # async def get_auth_config():
# #     api_key, api_secret = await TradeControl.get_config()
# #     return {"api_key": api_key, "api_secret": api_secret}

# # @app.get("/api/stats")
# # async def get_stats():
# #     return {
# #         "pnl": _compute_pnl(),
# #         "queue": RAM_STATE["tick_queue"].qsize() if RAM_STATE["tick_queue"] else 0,
# #         "inflight": len(RAM_STATE["inflight"]),
# #         "dropped": RAM_STATE["tick_batches_dropped"],
# #         "server_time": _now_ist().strftime("%H:%M:%S"),
# #         "engine_status": {k: "1" if v else "0" for k, v in RAM_STATE["engine_live"].items()},
# #         "data_connected": RAM_STATE["data_connected"],
# #         "order_updates_connected": RAM_STATE["order_updates_connected"],
# #     }

# # @app.get("/api/orders")
# # async def get_orders(open_only: int = 0):
# #     if not open_only:
# #         return RAM_STATE["trades"]
# #     return {side: [t for t in arr if _trade_is_open(t)] for side, arr in RAM_STATE["trades"].items()}

# # @app.get("/api/scanner")
# # async def get_scanner():
# #     signals = {side: [] for side in ["bull", "bear", "mom_bull", "mom_bear"]}
# #     for _, s in RAM_STATE["stocks"].items():
# #         for p in ["brk", "mom"]:
# #             if s.get(f"{p}_status") == "TRIGGER_WATCH":
# #                 side = s.get(f"{p}_side_latch")
# #                 if side in signals:
# #                     signals[side].append({
# #                         "symbol": s.get("symbol"),
# #                         "trigger_px": s.get(f"{p}_trigger_px") or s.get(f"{p}_trigger_high") or s.get(f"{p}_trigger_low"),
# #                         "seen_time": s.get(f"{p}_scan_seen_time") or _now_ist().strftime("%H:%M:%S")
# #                     })
# #     return signals

# # @app.get("/api/settings/engine/{side}")
# # async def get_settings(side: str):
# #     return RAM_STATE["config"].get(side, {})

# # @app.post("/api/settings/engine/{side}")
# # async def save_settings(side: str, data: Dict[str, Any]):
# #     if side in RAM_STATE["config"]:
# #         RAM_STATE["config"][side].update(data)
# #         await TradeControl.save_strategy_settings(side, RAM_STATE["config"][side])
# #     return {"status": "success"}

# # @app.post("/api/control")
# # async def control(request: Request):
# #     data = await request.json()
# #     action = data.get("action")

# #     if action == "toggle_engine":
# #         side, enabled = data.get("side"), data.get("enabled")
# #         if side in RAM_STATE["engine_live"]:
# #             RAM_STATE["engine_live"][side] = bool(enabled)

# #     elif action == "square_off_one":
# #         symbol, side = data.get("symbol"), data.get("side")
# #         stock = next((s for s in RAM_STATE["stocks"].values() if s.get("symbol") == symbol), None)
# #         if stock:
# #             if "mom" in side:
# #                 await MomentumEngine.close_position(stock, RAM_STATE, "USER_EXIT")
# #             else:
# #                 await BreakoutEngine.close_position(stock, RAM_STATE, "USER_EXIT")

# #     elif action == "square_off_all":
# #         side = data.get("side")
# #         for stock in RAM_STATE["stocks"].values():
# #             if side in ["bull", "bear"] and stock.get("brk_status") == "OPEN":
# #                 if stock.get("brk_side_latch") == side:
# #                     await BreakoutEngine.close_position(stock, RAM_STATE, "USER_EXIT_ALL")
# #             elif side in ["mom_bull", "mom_bear"] and stock.get("mom_status") == "OPEN":
# #                 if stock.get("mom_side_latch") == side:
# #                     await MomentumEngine.close_position(stock, RAM_STATE, "USER_EXIT_ALL")

# #     elif action == "save_api":
# #         client_id = data.get("api_key")
# #         access_token = data.get("api_secret")
# #         if client_id and access_token:
# #             success = await TradeControl.save_config(client_id, access_token)
# #             RAM_STATE["dhan_client_id"] = client_id
# #             RAM_STATE["dhan_access_token"] = access_token
# #             if success:
# #                 logger.info("✅ Dashboard: Dhan credentials saved to Redis and RAM.")
# #                 return {"status": "success"}
# #             return {"status": "error", "message": "Redis Save Failed"}

# #     return {"status": "ok"}

# # # -----------------------------
# # # STARTUP / SHUTDOWN
# # # -----------------------------
# # @app.on_event("startup")
# # async def startup_event():
# #     logger.info("🚀 System Startup")
# #     RAM_STATE["main_loop"] = asyncio.get_running_loop()
# #     RAM_STATE["tick_queue"] = asyncio.Queue(maxsize=10000)
# #     RAM_STATE["candle_close_queue"] = asyncio.Queue(maxsize=2000)
# #     RAM_STATE["engine_sem"] = asyncio.Semaphore(500)

# #     asyncio.create_task(tick_worker_parallel())
# #     asyncio.create_task(candle_worker())

# #     # Restore config from Redis (mapped fields)
# #     client_id, access_token = await TradeControl.get_config()
# #     RAM_STATE["dhan_client_id"] = (client_id or os.getenv("DHAN_CLIENT_ID", "")).strip()
# #     RAM_STATE["dhan_access_token"] = (access_token or os.getenv("DHAN_ACCESS_TOKEN", "")).strip()

# #     # Create Dhan client
# #     if RAM_STATE["dhan_client_id"] and RAM_STATE["dhan_access_token"]:
# #         try:
# #             if DhanContext is None:
# #                 raise RuntimeError("DhanContext not available in installed dhanhq. Upgrade: pip install -U dhanhq")
# #             ctx = DhanContext(RAM_STATE["dhan_client_id"], RAM_STATE["dhan_access_token"])
# #             RAM_STATE["dhan_context"] = ctx
# #             RAM_STATE["dhan"] = dhanhq(ctx)
# #             logger.info("✅ Dhan session ready.")
# #         except Exception as e:
# #             logger.error(f"❌ Failed to init Dhan session: {e}")

# #     # Load Universe (IMPORTANT: keys must be DHAN security_id now)
# #     market_data = await TradeControl.get_all_market_data()
# #     for sec_str, data in market_data.items():
# #         try:
# #             sec_id = int(sec_str)
# #         except Exception:
# #             continue
# #         RAM_STATE["stocks"][sec_id] = {
# #             **(data or {}),
# #             "token": sec_id,          # keep legacy field name
# #             "security_id": sec_id,
# #             "ltp": 0.0,
# #             "brk_status": "WAITING",
# #             "mom_status": "WAITING",
# #             "candle_1m": None
# #         }

# #     # Start MarketFeed + OrderUpdate threads
# #     if RAM_STATE.get("dhan_context") and RAM_STATE["stocks"]:
# #         if MarketFeed is None or OrderUpdate is None:
# #             logger.error("❌ Installed dhanhq missing MarketFeed/OrderUpdate. Upgrade: pip install -U dhanhq")
# #             return

# #         RAM_STATE["marketfeed_thread"] = threading.Thread(target=_marketfeed_thread_fn, daemon=True)
# #         RAM_STATE["marketfeed_thread"].start()

# #         RAM_STATE["orderupdate_thread"] = threading.Thread(target=_orderupdate_thread_fn, daemon=True)
# #         RAM_STATE["orderupdate_thread"].start()

# # @app.on_event("shutdown")
# # async def shutdown():
# #     try:
# #         mf = RAM_STATE.get("marketfeed")
# #         if mf:
# #             mf.disconnect()
# #     except Exception:
# #         pass
# #     RAM_STATE["data_connected"] = {"breakout": False, "momentum": False}
# #     RAM_STATE["order_updates_connected"] = False
# # -*- coding: utf-8 -*-
# """
# main.py (DHAN VERSION - FIXED)

# Fixes:
# - Removes any dependency on "DhanContext" (older dhanhq versions don't have it).
# - Uses only the supported public API:
#     from dhanhq import dhanhq
#     from dhanhq import marketfeed
# - Adds a safe fallback "poll fills" loop (so even if order-update websocket isn't available,
#   executed AvgTradedPrice can still be captured and target/trailing recalculated).
# - ASCII-only logs (prevents the unicode source decode crash you hit).

# Run:
#   uvicorn main:app --host 0.0.0.0 --port 8000

# Open in browser (from your PC):
#   http://157.20.215.106:8000/
# """

# import os
# import json
# import time
# import asyncio
# import logging
# import threading
# import inspect
# from datetime import datetime, timezone
# from typing import Any, Dict, List, Optional, Tuple

# import pytz
# from fastapi import FastAPI, HTTPException
# from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
# from fastapi.staticfiles import StaticFiles
# from pydantic import BaseModel

# from redis_manager import TradeControl

# # Engines
# from momentum_engine import MomentumEngine

# # Breakout engine is expected in your project.
# # If it's not present, the app will still run with Momentum only.
# try:
#     from breakout_engine import BreakoutEngine  # type: ignore
# except Exception:
#     BreakoutEngine = None  # type: ignore

# # DHAN SDK
# from dhanhq import dhanhq
# from dhanhq import marketfeed

# IST = pytz.timezone("Asia/Kolkata")

# # -----------------------------
# # LOGGING
# # -----------------------------
# LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
# logging.basicConfig(
#     level=LOG_LEVEL,
#     format="%(asctime)s [MAIN] %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
# )
# logger = logging.getLogger("MAIN")


# # -----------------------------
# # FASTAPI
# # -----------------------------
# app = FastAPI(title="Nexus DHAN Trading App")

# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# INDEX_HTML = os.path.join(BASE_DIR, "index.html")

# # If you have other static files later, put them in ./static and this will serve them.
# STATIC_DIR = os.path.join(BASE_DIR, "static")
# if os.path.isdir(STATIC_DIR):
#     app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# # -----------------------------
# # GLOBAL STATE (in-memory)
# # -----------------------------
# _state_lock = threading.RLock()

# STATE: Dict[str, Any] = {
#     "dhan": None,  # dhanhq instance
#     "client_id": "",
#     "access_token": "",

#     # market universe
#     "tokens": [],          # list[int] (security_id)
#     "stocks": {},          # token -> dict (market meta + runtime fields)
#     "symbol_to_token": {}, # symbol -> token

#     # strategy configs (loaded from redis)
#     "config": {
#         "bull": {},
#         "bear": {},
#         "mom_bull": {},
#         "mom_bear": {},
#     },

#     # trades (what UI reads)
#     "trades": {
#         "bull": [],
#         "bear": [],
#         "mom_bull": [],
#         "mom_bear": [],
#     },

#     # engine toggles (UI)
#     "engine_live": {
#         "bull": True,
#         "bear": True,
#         "mom_bull": True,
#         "mom_bear": True,
#     },

#     # feed connectivity
#     "data_connected": {
#         "breakout": False,
#         "momentum": False,
#     },

#     # manual exits from UI
#     "manual_exits": set(),

#     # candle state per token
#     "candle": {},  # token -> candle dict

#     # pending fill capture (fallback when orderupdate websocket isn't available)
#     "pending_orders": {},  # order_id -> (side_key, symbol)
# }


# # -----------------------------
# # REQUEST MODELS
# # -----------------------------
# class ControlRequest(BaseModel):
#     action: str
#     side: Optional[str] = None
#     enabled: Optional[bool] = None
#     symbol: Optional[str] = None

#     # broker creds
#     broker: Optional[str] = None
#     client_id: Optional[str] = None
#     access_token: Optional[str] = None

#     # backward compatible keys from UI
#     api_key: Optional[str] = None
#     api_secret: Optional[str] = None


# class EngineSettings(BaseModel):
#     risk_reward: Optional[str] = "1:2"
#     trailing_sl: Optional[str] = "1:1.5"
#     total_trades: Optional[int] = 5
#     risk_trade_1: Optional[float] = 2000.0
#     trade_start: Optional[str] = "09:15"
#     trade_end: Optional[str] = "15:10"
#     volume_criteria: Optional[list] = None


# # -----------------------------
# # SMALL HELPERS
# # -----------------------------
# def _now_ist() -> datetime:
#     return datetime.now(IST)


# def _safe_float(x: Any, d: float = 0.0) -> float:
#     try:
#         return float(x)
#     except Exception:
#         return d


# def _safe_int(x: Any, d: int = 0) -> int:
#     try:
#         return int(x)
#     except Exception:
#         return d


# def _parse_rr(rr: str, default: float = 2.0) -> float:
#     try:
#         # "1:2" -> 2
#         return float(str(rr).split(":")[-1])
#     except Exception:
#         return default


# def _parse_tsl(tsl: str, default: float = 1.5) -> float:
#     try:
#         return float(str(tsl).split(":")[-1])
#     except Exception:
#         return default


# def _is_long_side(side_key: str) -> bool:
#     return side_key in ("bull", "mom_bull")


# def _extract_order_status(resp: Any) -> str:
#     """
#     Best-effort for dhanhq order response formats.
#     """
#     if isinstance(resp, dict):
#         for k in ("orderStatus", "status", "OrderStatus", "Status"):
#             v = resp.get(k)
#             if v:
#                 return str(v).upper()
#         data = resp.get("data")
#         if isinstance(data, dict):
#             for k in ("orderStatus", "status", "OrderStatus", "Status"):
#                 v = data.get(k)
#                 if v:
#                     return str(v).upper()
#     return ""


# def _extract_avg_price(resp: Any) -> Optional[float]:
#     """
#     Best-effort for dhanhq order response formats.
#     """
#     if isinstance(resp, dict):
#         keys = [
#             "avgTradedPrice",
#             "AvgTradedPrice",
#             "averageTradedPrice",
#             "AverageTradedPrice",
#             "avg_price",
#             "avgPrice",
#             "AvgPrice",
#             "tradedPrice",
#         ]
#         for k in keys:
#             if resp.get(k) is not None:
#                 v = _safe_float(resp.get(k), 0.0)
#                 if v > 0:
#                     return v
#         data = resp.get("data")
#         if isinstance(data, dict):
#             for k in keys:
#                 if data.get(k) is not None:
#                     v = _safe_float(data.get(k), 0.0)
#                     if v > 0:
#                         return v
#     return None


# def _extract_filled_qty(resp: Any) -> Optional[int]:
#     if isinstance(resp, dict):
#         keys = ["tradedQty", "TradedQty", "filledQty", "FilledQty", "filled_quantity"]
#         for k in keys:
#             if resp.get(k) is not None:
#                 v = _safe_int(resp.get(k), 0)
#                 if v > 0:
#                     return v
#         data = resp.get("data")
#         if isinstance(data, dict):
#             for k in keys:
#                 if data.get(k) is not None:
#                     v = _safe_int(data.get(k), 0)
#                     if v > 0:
#                         return v
#     return None


# def _find_trade_by_order_id(order_id: str) -> Optional[Tuple[str, Dict[str, Any]]]:
#     """
#     Returns (side_key, trade_dict_ref) for an OPEN trade.
#     """
#     oid = str(order_id or "").strip()
#     if not oid:
#         return None
#     with _state_lock:
#         for side_key, trades in STATE["trades"].items():
#             for t in trades:
#                 if str(t.get("order_id")) == oid and str(t.get("status", "")).upper() == "OPEN":
#                     return side_key, t
#     return None


# def _recompute_trade_from_exec(trade: Dict[str, Any], side_key: str, exec_px: float) -> None:
#     """
#     IMPORTANT:
#     Target + Trailing must be based on EXECUTED price (AvgTradedPrice).
#     This function overwrites trade.entry_price and recomputes SL/Target/Trail step.
#     """
#     if exec_px <= 0:
#         return

#     is_long = _is_long_side(side_key)

#     sl_pct = _safe_float(trade.get("sl_pct"), 0.0)
#     rr_val = _safe_float(trade.get("rr_val"), 0.0)
#     tsl_ratio = _safe_float(trade.get("tsl_ratio"), 0.0)

#     # Fallbacks if not present
#     if sl_pct <= 0:
#         sl_pct = 0.005
#     if rr_val <= 0:
#         rr_val = 2.0
#     if tsl_ratio <= 0:
#         tsl_ratio = 1.5

#     sl_px = round(exec_px * (1.0 - sl_pct), 2) if is_long else round(exec_px * (1.0 + sl_pct), 2)
#     risk = max(abs(exec_px - sl_px), exec_px * 0.0005)

#     target = round(exec_px + (risk * rr_val), 2) if is_long else round(exec_px - (risk * rr_val), 2)
#     trail_step = float(risk * tsl_ratio) if tsl_ratio > 0 else float(risk)

#     trade["entry_exec_price"] = float(exec_px)
#     trade["entry_price"] = float(exec_px)

#     trade["sl_price"] = float(sl_px)
#     trade["init_risk"] = float(risk)

#     trade["target_price"] = float(target)
#     trade["trail_step"] = float(trail_step)


# # -----------------------------
# # LOAD CONFIG + UNIVERSE
# # -----------------------------
# async def load_engine_settings_from_redis() -> None:
#     for side in ("bull", "bear", "mom_bull", "mom_bear"):
#         cfg = await TradeControl.get_strategy_settings(side)
#         if not isinstance(cfg, dict):
#             cfg = {}
#         # defaults if missing
#         cfg.setdefault("risk_reward", "1:2")
#         cfg.setdefault("trailing_sl", "1:1.5")
#         cfg.setdefault("total_trades", 5)
#         cfg.setdefault("risk_trade_1", 2000)
#         if side in ("bull", "bear"):
#             cfg.setdefault("trade_start", "09:15")
#             cfg.setdefault("trade_end", "15:10")
#         cfg.setdefault("volume_criteria", [])
#         with _state_lock:
#             STATE["config"][side] = cfg


# async def load_universe_from_redis() -> None:
#     tokens = await TradeControl.get_subscribe_universe_tokens()
#     md_map = await TradeControl.get_all_market_data()

#     stocks: Dict[int, Dict[str, Any]] = {}
#     sym_to_token: Dict[str, int] = {}

#     for t in tokens:
#         t_str = str(int(t))
#         md = md_map.get(t_str) or {}
#         sym = str(md.get("symbol") or "").strip().upper()
#         if not sym:
#             continue
#         md["token"] = int(t)
#         md["security_id"] = int(t)
#         md.setdefault("ltp", 0.0)

#         # runtime fields used by engines / UI
#         md.setdefault("brk_status", "WAITING")
#         md.setdefault("mom_status", "WAITING")

#         stocks[int(t)] = md
#         sym_to_token[sym] = int(t)

#     with _state_lock:
#         STATE["tokens"] = list(stocks.keys())
#         STATE["stocks"] = stocks
#         STATE["symbol_to_token"] = sym_to_token

#     logger.info("Universe loaded. tokens=%d", len(stocks))


# # -----------------------------
# # DHAN SESSION INIT (NO DhanContext)
# # -----------------------------
# async def init_dhan_session_from_redis() -> bool:
#     client_id, api_secret = await TradeControl.get_config()
#     access_token = await TradeControl.get_access_token()

#     # Some users stored token in config:api_secret (older UI) - try fallback
#     if not access_token:
#         access_token = str(api_secret or "")

#     client_id = str(client_id or "").strip()
#     access_token = str(access_token or "").strip()

#     if not client_id or not access_token:
#         logger.warning("Missing DHAN client_id or access_token in Redis. Open UI -> API Gateway -> Save.")
#         return False

#     try:
#         dhan = dhanhq(client_id, access_token)
#         with _state_lock:
#             STATE["dhan"] = dhan
#             STATE["client_id"] = client_id
#             STATE["access_token"] = access_token
#         logger.info("Dhan session initialized (dhanhq).")
#         return True
#     except Exception as e:
#         logger.error("Failed to init Dhan session (dhanhq): %s", e)
#         return False


# # -----------------------------
# # TICK -> CANDLE + ENGINE DISPATCH
# # -----------------------------
# def _bucket_minute_ist(ts: Optional[datetime]) -> datetime:
#     dt = ts or _now_ist()
#     if dt.tzinfo is None:
#         dt = dt.replace(tzinfo=IST)
#     else:
#         dt = dt.astimezone(IST)
#     return dt.replace(second=0, microsecond=0)


# async def _on_candle_close(token: int, candle: Dict[str, Any]) -> None:
#     """
#     Called when a 1-minute candle completes.
#     """
#     # Breakout first (if present)
#     if BreakoutEngine is not None:
#         try:
#             await BreakoutEngine.on_candle_close(token, candle, STATE)  # type: ignore
#         except Exception as e:
#             logger.exception("BreakoutEngine.on_candle_close error: %s", e)

#     # Momentum first candle logic uses on_candle_close too
#     try:
#         await MomentumEngine.on_candle_close(token, candle, STATE)
#     except Exception as e:
#         logger.exception("MomentumEngine.on_candle_close error: %s", e)


# async def _dispatch_tick(token: int, ltp: float, vol: int, tick_ts: Optional[datetime] = None) -> None:
#     """
#     Central tick handler: candle build + engines run.
#     """
#     with _state_lock:
#         stock = STATE["stocks"].get(token)
#         if not stock:
#             return

#     # Update scanner "seen time" if in TRIGGER_WATCH
#     now = _now_ist()
#     with _state_lock:
#         if str(stock.get("brk_status", "")).upper() == "TRIGGER_WATCH":
#             if not stock.get("brk_scan_seen_ts"):
#                 stock["brk_scan_seen_ts"] = now.timestamp()
#                 stock["brk_scan_seen_time"] = now.strftime("%H:%M:%S")
#         if str(stock.get("mom_status", "")).upper() == "TRIGGER_WATCH":
#             if not stock.get("mom_scan_seen_ts"):
#                 stock["mom_scan_seen_ts"] = now.timestamp()
#                 stock["mom_scan_seen_time"] = now.strftime("%H:%M:%S")

#     # Candle build
#     bucket = _bucket_minute_ist(tick_ts)
#     close_prev: Optional[Dict[str, Any]] = None

#     with _state_lock:
#         c = STATE["candle"].get(token)
#         if not c:
#             STATE["candle"][token] = {
#                 "bucket": bucket.isoformat(),
#                 "open": float(ltp),
#                 "high": float(ltp),
#                 "low": float(ltp),
#                 "close": float(ltp),
#                 "volume": int(vol or 0),
#                 "last_vol": int(vol or 0),
#             }
#         else:
#             # minute changed -> close candle
#             prev_bucket = None
#             try:
#                 prev_bucket = datetime.fromisoformat(str(c.get("bucket"))).astimezone(IST)
#             except Exception:
#                 prev_bucket = None

#             if prev_bucket and prev_bucket != bucket:
#                 close_prev = dict(c)
#                 # start new candle
#                 STATE["candle"][token] = {
#                     "bucket": bucket.isoformat(),
#                     "open": float(ltp),
#                     "high": float(ltp),
#                     "low": float(ltp),
#                     "close": float(ltp),
#                     "volume": 0,
#                     "last_vol": int(vol or 0),
#                 }
#             else:
#                 c["close"] = float(ltp)
#                 c["high"] = float(max(_safe_float(c.get("high")), ltp))
#                 c["low"] = float(min(_safe_float(c.get("low"), ltp), ltp))

#                 # If vol is "day volume", store per-minute delta
#                 last_vol = _safe_int(c.get("last_vol"), int(vol or 0))
#                 cur_vol = int(vol or 0)
#                 delta = cur_vol - last_vol
#                 if delta < 0:
#                     delta = 0
#                 c["volume"] = int(_safe_int(c.get("volume"), 0) + delta)
#                 c["last_vol"] = cur_vol

#     # candle close callbacks (outside lock)
#     if close_prev:
#         await _on_candle_close(token, close_prev)

#     # Engine tick dispatch
#     # Breakout
#     if BreakoutEngine is not None:
#         try:
#             await BreakoutEngine.run(token, float(ltp), int(vol or 0), STATE)  # type: ignore
#             with _state_lock:
#                 STATE["data_connected"]["breakout"] = True
#         except Exception as e:
#             logger.exception("BreakoutEngine.run error: %s", e)

#     # Momentum
#     try:
#         await MomentumEngine.run(token, float(ltp), int(vol or 0), STATE)
#         with _state_lock:
#             STATE["data_connected"]["momentum"] = True
#     except Exception as e:
#         logger.exception("MomentumEngine.run error: %s", e)


# # -----------------------------
# # DHAN MARKET FEED WORKER
# # -----------------------------
# def _exchange_code_for_equity() -> Any:
#     """
#     dhanhq marketfeed constants differ by version.
#     Prefer marketfeed.NSE when available; else use numeric 1 (as per old docs).
#     """
#     return getattr(marketfeed, "NSE", 1)


# def _ticker_sub_code() -> Any:
#     return getattr(marketfeed, "Ticker", 15)  # 15 is what many libs use for "ticker" mode


# def _build_instruments(tokens: List[int]) -> List[tuple]:
#     ex = _exchange_code_for_equity()
#     sub = _ticker_sub_code()

#     instruments: List[tuple] = []
#     # Some dhanhq versions require (exchange, security_id) and subscription passed separately.
#     # Some accept (exchange, security_id, subscription_type).
#     for t in tokens:
#         if hasattr(marketfeed, "Ticker"):
#             # supports 3-tuple
#             instruments.append((ex, str(int(t)), sub))
#         else:
#             instruments.append((ex, str(int(t))))
#     return instruments


# def _init_dhanfeed(client_id: str, access_token: str, instruments: List[tuple]):
#     """
#     Works across multiple dhanhq versions by inspecting signature.
#     """
#     sig = None
#     try:
#         sig = inspect.signature(marketfeed.DhanFeed)
#     except Exception:
#         sig = None

#     # Old README version (example):
#     #   DhanFeed(client_id, access_token, instruments, subscription_code, on_connect=..., on_message=...)
#     # Newer versions sometimes take:
#     #   DhanFeed(client_id, access_token, instruments, version)
#     kwargs = {}

#     def on_connect():
#         logger.info("DHAN marketfeed websocket connected. subscribed=%d", len(instruments))

#     async def on_message(_ws, message):
#         # message can be dict or list[dict]
#         try:
#             await _handle_marketfeed_message(message)
#         except Exception as e:
#             logger.exception("on_message error: %s", e)

#     if sig:
#         params = list(sig.parameters.keys())
#         if "subscription_code" in params or (len(params) >= 4 and params[3] in ("subscription_code", "subscriptionCode")):
#             # old style: pass subscription_code separately, instruments as (exchange, id)
#             sub_code = _ticker_sub_code()
#             kwargs.update({"on_connect": on_connect, "on_message": on_message})
#             return marketfeed.DhanFeed(client_id, access_token, instruments, sub_code, **kwargs)

#         # if it has "version" param
#         if "version" in params or (len(params) >= 4 and params[3] == "version"):
#             # best guess: use "v2"
#             return marketfeed.DhanFeed(client_id, access_token, instruments, "v2")

#     # fallback: try most common constructor
#     try:
#         return marketfeed.DhanFeed(client_id, access_token, instruments, _ticker_sub_code(), on_connect=on_connect, on_message=on_message)
#     except Exception:
#         return marketfeed.DhanFeed(client_id, access_token, instruments)


# async def _handle_marketfeed_message(message: Any) -> None:
#     """
#     Normalizes tick packets into (token, ltp, volume, ts) and sends to dispatcher.
#     """
#     if message is None:
#         return

#     msgs: List[dict] = []
#     if isinstance(message, dict):
#         msgs = [message]
#     elif isinstance(message, list):
#         msgs = [m for m in message if isinstance(m, dict)]
#     else:
#         return

#     for m in msgs:
#         # token/securityId keys vary by version
#         token = _safe_int(
#             m.get("securityId")
#             or m.get("security_id")
#             or m.get("SecurityId")
#             or m.get("token")
#             or m.get("Token"),
#             0,
#         )
#         if token <= 0:
#             continue

#         ltp = _safe_float(
#             m.get("ltp")
#             or m.get("LTP")
#             or m.get("lastTradedPrice")
#             or m.get("LastTradedPrice")
#             or m.get("price"),
#             0.0,
#         )
#         if ltp <= 0:
#             continue

#         # Many feeds provide cumulative day volume.
#         vol = _safe_int(
#             m.get("volume")
#             or m.get("Volume")
#             or m.get("dayVolume")
#             or m.get("DayVolume")
#             or m.get("ttv"),  # sometimes total traded volume
#             0,
#         )

#         # timestamp (if available)
#         ts: Optional[datetime] = None
#         raw_ts = m.get("timestamp") or m.get("ts") or m.get("time")
#         if raw_ts:
#             try:
#                 # epoch seconds or ms
#                 v = int(float(raw_ts))
#                 if v > 10_000_000_000:  # ms
#                     ts = datetime.fromtimestamp(v / 1000.0, tz=timezone.utc).astimezone(IST)
#                 else:  # seconds
#                     ts = datetime.fromtimestamp(v, tz=timezone.utc).astimezone(IST)
#             except Exception:
#                 ts = None

#         await _dispatch_tick(token, ltp, vol, ts)


# def _marketfeed_thread_main() -> None:
#     """
#     Runs DHAN marketfeed websocket in a dedicated thread.
#     """
#     try:
#         # Create an event loop for this thread (needed because on_message is async)
#         loop = asyncio.new_event_loop()
#         asyncio.set_event_loop(loop)

#         with _state_lock:
#             client_id = STATE["client_id"]
#             access_token = STATE["access_token"]
#             tokens = list(STATE["tokens"])

#         if not tokens:
#             logger.warning("No tokens in universe. Run sync_market_data.py first.")
#             return

#         instruments = _build_instruments(tokens)
#         feed = _init_dhanfeed(client_id, access_token, instruments)

#         logger.info("Marketfeed worker starting...")
#         # Many dhanhq versions implement run_forever() as blocking.
#         # It will run inside this thread.
#         feed.run_forever()

#     except Exception as e:
#         logger.error("Marketfeed thread crashed: %s", e, exc_info=True)
#     finally:
#         with _state_lock:
#             STATE["data_connected"]["breakout"] = False
#             STATE["data_connected"]["momentum"] = False
#         logger.warning("Marketfeed worker stopped.")


# # -----------------------------
# # FALLBACK: POLL FILLS (AvgTradedPrice)
# # -----------------------------
# async def _poll_fills_loop() -> None:
#     """
#     If you don't have order-update websocket working, this loop still captures
#     executed avg price and recalculates target/trailing based on execution.
#     """
#     while True:
#         await asyncio.sleep(2.0)

#         with _state_lock:
#             dhan = STATE.get("dhan")
#             pending = list(STATE["pending_orders"].keys())

#         if not dhan or not pending:
#             continue

#         for oid in pending:
#             try:
#                 resp = await asyncio.to_thread(dhan.get_order_by_id, oid)
#                 status = _extract_order_status(resp)
#                 avg_px = _extract_avg_price(resp)
#                 filled = _extract_filled_qty(resp)

#                 if status in ("TRADED", "FILLED", "COMPLETE", "COMPLETED", "EXECUTED") and avg_px and avg_px > 0:
#                     found = _find_trade_by_order_id(oid)
#                     if found:
#                         side_key, trade = found
#                         _recompute_trade_from_exec(trade, side_key, float(avg_px))
#                         logger.info("Order fill captured. order_id=%s avg=%.2f qty=%s", oid, avg_px, filled)

#                     with _state_lock:
#                         STATE["pending_orders"].pop(oid, None)

#             except Exception:
#                 # ignore transient errors
#                 continue


# # -----------------------------
# # STARTUP
# # -----------------------------
# @app.on_event("startup")
# async def on_startup() -> None:
#     # Load configs and universe first
#     await load_engine_settings_from_redis()
#     await load_universe_from_redis()

#     # Init dhan session
#     ok = await init_dhan_session_from_redis()
#     if not ok:
#         # App will still serve UI, but no live feed
#         return

#     # Start fill polling fallback
#     asyncio.create_task(_poll_fills_loop())

#     # Start marketfeed worker in a thread
#     t = threading.Thread(target=_marketfeed_thread_main, daemon=True)
#     t.start()
#     logger.info("Parallel tick worker active.")


# # -----------------------------
# # ROUTES
# # -----------------------------
# @app.get("/", response_class=HTMLResponse)
# async def home() -> Any:
#     if not os.path.exists(INDEX_HTML):
#         return HTMLResponse("<h3>index.html not found</h3>", status_code=404)
#     return FileResponse(INDEX_HTML)


# @app.get("/api/config/auth")
# async def get_auth() -> Any:
#     client_id, api_secret = await TradeControl.get_config()
#     access_token = await TradeControl.get_access_token()
#     # fallback if old storage used
#     if not access_token:
#         access_token = str(api_secret or "")
#     return {
#         "client_id": client_id,
#         "access_token": access_token,
#         # backward compatible
#         "api_key": client_id,
#         "api_secret": access_token,
#     }


# @app.get("/api/settings/engine/{side}")
# async def get_engine_settings(side: str) -> Any:
#     side = str(side or "").strip().lower()
#     if side not in ("bull", "bear", "mom_bull", "mom_bear"):
#         raise HTTPException(status_code=400, detail="invalid side")
#     cfg = await TradeControl.get_strategy_settings(side)
#     return cfg or {}


# @app.post("/api/settings/engine/{side}")
# async def save_engine_settings(side: str, cfg: EngineSettings) -> Any:
#     side = str(side or "").strip().lower()
#     if side not in ("bull", "bear", "mom_bull", "mom_bear"):
#         raise HTTPException(status_code=400, detail="invalid side")

#     data = cfg.model_dump() if hasattr(cfg, "model_dump") else cfg.dict()  # pydantic v1/v2

#     # Store in redis
#     ok = await TradeControl.save_strategy_settings(side, data)
#     if not ok:
#         raise HTTPException(status_code=500, detail="failed to save")

#     # Update in-memory too
#     with _state_lock:
#         STATE["config"][side] = data

#     return {"status": "ok"}


# @app.get("/api/orders")
# async def api_orders() -> Any:
#     with _state_lock:
#         return STATE["trades"]


# @app.get("/api/scanner")
# async def api_scanner() -> Any:
#     out = {"bull": [], "bear": [], "mom_bull": [], "mom_bear": []}

#     with _state_lock:
#         stocks = list(STATE["stocks"].values())

#     for s in stocks:
#         sym = str(s.get("symbol") or "").strip().upper()
#         if not sym:
#             continue

#         # Breakout scan candidates
#         if str(s.get("brk_status") or "").upper() == "TRIGGER_WATCH":
#             side = str(s.get("brk_side_latch") or "").lower() or ("bull" if _safe_float(s.get("pdh")) > 0 else "bull")
#             if side not in ("bull", "bear"):
#                 side = "bull"
#             out[side].append({
#                 "symbol": sym,
#                 "trigger_px": _safe_float(s.get("brk_trigger_px") or s.get("pdh") or s.get("pdl"), 0.0),
#                 "seen_time": s.get("brk_scan_seen_time"),
#                 "reason": s.get("brk_scan_reason"),
#             })

#         # Momentum scan candidates
#         if str(s.get("mom_status") or "").upper() == "TRIGGER_WATCH":
#             # momentum doesn't latch side until break; still show both lists in one (UI shows per-side panel anyway)
#             # We'll put it in both mom_bull/mom_bear if both triggers exist; UI display is your choice.
#             out["mom_bull"].append({
#                 "symbol": sym,
#                 "trigger_px": _safe_float(s.get("mom_trigger_high"), 0.0),
#                 "seen_time": s.get("mom_scan_seen_time"),
#                 "reason": s.get("mom_scan_reason"),
#             })
#             out["mom_bear"].append({
#                 "symbol": sym,
#                 "trigger_px": _safe_float(s.get("mom_trigger_low"), 0.0),
#                 "seen_time": s.get("mom_scan_seen_time"),
#                 "reason": s.get("mom_scan_reason"),
#             })

#     return out


# @app.get("/api/stats")
# async def api_stats() -> Any:
#     pnl_side = {"bull": 0.0, "bear": 0.0, "mom_bull": 0.0, "mom_bear": 0.0}
#     total = 0.0

#     with _state_lock:
#         trades = json.loads(json.dumps(STATE["trades"]))  # cheap deep copy for safety
#         engine_live = dict(STATE["engine_live"])
#         data_connected = dict(STATE["data_connected"])

#     for side, tlist in trades.items():
#         s = 0.0
#         for t in tlist:
#             if str(t.get("status", "")).upper() == "OPEN":
#                 s += _safe_float(t.get("pnl"), 0.0)
#         pnl_side[side] = round(s, 2)
#         total += s

#     return {
#         "pnl": {**pnl_side, "total": round(total, 2)},
#         "engine_status": {k: ("1" if v else "0") for k, v in engine_live.items()},
#         "data_connected": data_connected,
#     }


# @app.post("/api/control")
# async def api_control(req: ControlRequest) -> Any:
#     action = str(req.action or "").strip().lower()

#     if action == "save_api":
#         # Accept both new + backward compatible payload keys
#         client_id = (req.client_id or req.api_key or "").strip()
#         access_token = (req.access_token or req.api_secret or "").strip()

#         if not client_id or not access_token:
#             raise HTTPException(status_code=400, detail="missing client_id/access_token")

#         ok1 = await TradeControl.save_config(client_id, access_token)
#         ok2 = await TradeControl.save_access_token(access_token)
#         if not (ok1 and ok2):
#             raise HTTPException(status_code=500, detail="failed to save creds")

#         # re-init dhan session in memory
#         await init_dhan_session_from_redis()
#         return {"status": "success"}

#     if action == "toggle_engine":
#         side = str(req.side or "").strip().lower()
#         if side not in ("bull", "bear", "mom_bull", "mom_bear"):
#             raise HTTPException(status_code=400, detail="invalid side")
#         enabled = bool(req.enabled)
#         with _state_lock:
#             STATE["engine_live"][side] = enabled
#         return {"status": "ok"}

#     if action == "square_off_one":
#         side = str(req.side or "").strip().lower()
#         symbol = str(req.symbol or "").strip().upper()
#         if side not in ("bull", "bear", "mom_bull", "mom_bear") or not symbol:
#             raise HTTPException(status_code=400, detail="invalid side/symbol")

#         # mark manual exit; engine monitor loop will exit on next tick
#         with _state_lock:
#             STATE["manual_exits"].add(symbol)
#         return {"status": "ok"}

#     if action == "square_off_all":
#         side = str(req.side or "").strip().lower()
#         if side not in ("bull", "bear", "mom_bull", "mom_bear"):
#             raise HTTPException(status_code=400, detail="invalid side")

#         # set manual exits for all open trades in that side
#         with _state_lock:
#             for t in STATE["trades"].get(side, []):
#                 if str(t.get("status", "")).upper() == "OPEN":
#                     sym = str(t.get("symbol") or "").strip().upper()
#                     if sym:
#                         STATE["manual_exits"].add(sym)
#         return {"status": "ok"}

#     raise HTTPException(status_code=400, detail="unknown action")


# # -----------------------------
# # HOOK: when engines place entry orders, add to pending_orders for fill capture
# # (MomentumEngine already appends trade; we just watch trades list periodically.)
# # -----------------------------
# async def _pending_orders_watcher() -> None:
#     """
#     Optional helper: if some engine code doesn't explicitly add pending order ids,
#     this watcher detects new OPEN trades and adds their order_id to pending_orders.
#     """
#     seen: set = set()
#     while True:
#         await asyncio.sleep(1.0)
#         with _state_lock:
#             for side_key, tlist in STATE["trades"].items():
#                 for t in tlist:
#                     oid = str(t.get("order_id") or "").strip()
#                     if not oid:
#                         continue
#                     if oid in seen:
#                         continue
#                     if str(t.get("status", "")).upper() != "OPEN":
#                         continue
#                     # only if exec price not known yet
#                     if _safe_float(t.get("entry_exec_price"), 0.0) > 0:
#                         continue
#                     seen.add(oid)
#                     STATE["pending_orders"][oid] = (side_key, str(t.get("symbol") or ""))
#                     logger.info("Pending fill tracking added. order_id=%s side=%s", oid, side_key)


# @app.on_event("startup")
# async def _startup_extra() -> None:
#     asyncio.create_task(_pending_orders_watcher())
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

# Your redis wrapper (must exist in your project)
from redis_manager import TradeControl


# -----------------------------
# Logging (ASCII-only to avoid UnicodeDecodeError)
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
DHAN_WS_AUTH_TYPE = os.getenv("DHAN_WS_AUTH_TYPE", "2")  # per docs default 2 :contentReference[oaicite:3]{index=3}

# Subscribe mode:
# RequestCode 15 = Ticker, 17 = Quote, 21 = Full (refer feed request code) :contentReference[oaicite:4]{index=4}
DHAN_SUBSCRIBE_REQUEST_CODE = int(os.getenv("DHAN_SUBSCRIBE_REQUEST_CODE", "17"))

# Dhan per-message instrument limit
DHAN_SUBSCRIBE_CHUNK = int(os.getenv("DHAN_SUBSCRIBE_CHUNK", "100"))  # docs limit :contentReference[oaicite:5]{index=5}

# Optional small sleep between subscription batches (prevents burst)
SUBSCRIBE_BATCH_SLEEP = float(os.getenv("SUBSCRIBE_BATCH_SLEEP", "0.05"))

# Tick queues
TICK_QUEUE_MAX = int(os.getenv("TICK_QUEUE_MAX", "20000"))

# UI expected engine sides
ENGINE_SIDES = ["bull", "mom_bull", "bear", "mom_bear"]


# -----------------------------
# Binary parsing (Dhan WS is little-endian) :contentReference[oaicite:6]{index=6}
# Header: 8 bytes -> code(1), length(2), exch(1), secid(4) :contentReference[oaicite:7]{index=7}
# -----------------------------
HDR_STRUCT = struct.Struct("<BHBI")  # 1 + 2 + 1 + 4 = 8 bytes

# Ticker packet (code 2): ltp(float32), ltt(int32 epoch) :contentReference[oaicite:8]{index=8}
TICKER_STRUCT = struct.Struct("<fI")

# PrevClose packet (code 6): prev_close(float32), oi(int32) :contentReference[oaicite:9]{index=9}
PREVCLOSE_STRUCT = struct.Struct("<fI")

# Quote packet (code 4): :contentReference[oaicite:10]{index=10}
# ltp(f), ltq(h), ltt(I), atp(f), vol(I), tsq(I), tbq(I), open(f), close(f), high(f), low(f)
QUOTE_STRUCT = struct.Struct("<f h I f I I I f f f f")


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
# Redis helpers (robust to differing TradeControl implementations)
# -----------------------------
async def _get_redis_client():
    """
    Best-effort: use TradeControl's redis if exposed; otherwise create our own.
    """
    # Many projects keep an internal client on TradeControl.redis / TradeControl._redis
    for attr in ("redis", "_redis", "client"):
        r = getattr(TradeControl, attr, None)
        if r:
            return r

    # fallback: create our own redis client
    try:
        import redis.asyncio as redis  # type: ignore
        return redis.from_url(REDIS_URL, decode_responses=True)
    except Exception as e:
        raise RuntimeError(f"Redis client not available: {e}") from e


async def _load_universe_from_redis() -> Tuple[List[int], List[str]]:
    """
    Try TradeControl method first; fallback to keys used by sync script:
      nexus:universe:tokens
      nexus:universe:symbols
    """
    # 1) Try TradeControl methods if present
    for meth in ("get_subscribe_universe", "get_universe", "load_universe"):
        fn = getattr(TradeControl, meth, None)
        if callable(fn):
            data = await fn()
            # expected either tuple or dict
            if isinstance(data, tuple) and len(data) == 2:
                return list(map(int, data[0] or [])), list(map(str, data[1] or []))
            if isinstance(data, dict):
                return list(map(int, data.get("tokens") or [])), list(map(str, data.get("symbols") or []))

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
            # maybe stored as csv
            tokens = [int(x) for x in str(tokens_raw).split(",") if x.strip().isdigit()]

    if syms_raw:
        try:
            symbols = [str(x) for x in json.loads(syms_raw)]
        except Exception:
            symbols = [x.strip().upper() for x in str(syms_raw).split(",") if x.strip()]

    return tokens, symbols


async def _load_market_data_bulk(tokens: List[int]) -> Dict[int, Dict[str, Any]]:
    """
    Try TradeControl.get_market_data(token) if present; fallback to nexus:market_data:{token}.
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

            raw = await r.get(f"nexus:market_data:{t}")
            if raw:
                out[int(t)] = json.loads(raw)
        except Exception:
            continue
    return out


async def _load_engine_status() -> None:
    """
    Engine toggles can be stored in Redis; keep robust.
    """
    r = await _get_redis_client()
    for side in ENGINE_SIDES:
        v = await r.get(f"nexus:engine:{side}:enabled")
        if v is not None:
            STATE.engine_status[side] = "1" if str(v) in ("1", "true", "True") else "0"


async def _save_engine_status(side: str, enabled: bool) -> None:
    r = await _get_redis_client()
    await r.set(f"nexus:engine:{side}:enabled", "1" if enabled else "0")
    STATE.engine_status[side] = "1" if enabled else "0"


# -----------------------------
# Dhan WebSocket worker
# -----------------------------
def _build_dhan_ws_url(access_token: str, client_id: str) -> str:
    # As per Dhan v2 docs :contentReference[oaicite:11]{index=11}
    return (
        f"{DHAN_WS_BASE}"
        f"?version=2"
        f"&token={access_token}"
        f"&clientId={client_id}"
        f"&authType={DHAN_WS_AUTH_TYPE}"
    )


def _chunk_instruments(tokens: List[int]) -> List[List[int]]:
    return [tokens[i : i + DHAN_SUBSCRIBE_CHUNK] for i in range(0, len(tokens), DHAN_SUBSCRIBE_CHUNK)]


async def _send_subscriptions(ws: WebSocketClientProtocol, tokens: List[int]) -> None:
    """
    Send multiple JSON messages (100 instruments max per message) :contentReference[oaicite:12]{index=12}
    """
    batches = _chunk_instruments(tokens)
    for batch in batches:
        payload = {
            "RequestCode": DHAN_SUBSCRIBE_REQUEST_CODE,
            "InstrumentCount": len(batch),
            "InstrumentList": [
                {"ExchangeSegment": "NSE_EQ", "SecurityId": str(t)} for t in batch
            ],
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
    See Dhan Live Market Feed binary specs :contentReference[oaicite:13]{index=13}
    """
    if not data or len(data) < 8:
        return None

    try:
        feed_code, msg_len, exch, sec_id = HDR_STRUCT.unpack_from(data, 0)
    except Exception:
        return None

    token = int(sec_id)
    symbol = STATE.token_to_symbol.get(token, str(token))

    # code 2 = ticker packet :contentReference[oaicite:14]{index=14}
    if feed_code == 2 and len(data) >= 8 + TICKER_STRUCT.size:
        ltp, ltt = TICKER_STRUCT.unpack_from(data, 8)
        return Tick(token=token, symbol=symbol, ltp=float(ltp), ltt_epoch=int(ltt))

    # code 4 = quote packet :contentReference[oaicite:15]{index=15}
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

    # code 6 = prev close :contentReference[oaicite:16]{index=16}
    if feed_code == 6 and len(data) >= 8 + PREVCLOSE_STRUCT.size:
        prev_close, oi = PREVCLOSE_STRUCT.unpack_from(data, 8)
        # we store prev_close into STATE.market_data cache too
        md = STATE.market_data.get(token) or {}
        md["prev_close"] = float(prev_close)
        STATE.market_data[token] = md
        return None

    # code 50 = disconnect packet (server side) :contentReference[oaicite:17]{index=17}
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
            # pull creds from redis via TradeControl (your existing methods)
            client_id, _ = await TradeControl.get_config()
            access_token = await TradeControl.get_access_token()

            if not client_id or not access_token:
                STATE.dhan_connected = False
                logger.error("Dhan creds missing in Redis (client_id/access_token).")
                await asyncio.sleep(2.0)
                continue

            url = _build_dhan_ws_url(access_token, client_id)
            logger.info("Marketfeed worker connecting...")

            # Connect (server pings every 10s; library auto-pongs) :contentReference[oaicite:18]{index=18}
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
                            # non-blocking put; drop if overloaded (keeps latency low)
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
            # request disconnect (RequestCode 12) :contentReference[oaicite:19]{index=19}
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
    # keep last 200 signals per side
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

        # Simple scanner example using PDH/PDL from market_data (from sync_market_data.py)
        md = STATE.market_data.get(tick.token)
        if md:
            pdh = float(md.get("pdh") or 0.0)
            pdl = float(md.get("pdl") or 0.0)

            if pdh > 0 and tick.ltp >= pdh:
                _push_scanner_signal("bull", tick.symbol, tick.ltp, "LTP >= PDH")

            if pdl > 0 and tick.ltp <= pdl:
                _push_scanner_signal("bear", tick.symbol, tick.ltp, "LTP <= PDL")

        # NOTE:
        # If you have BreakoutEngine/MomentumEngine, call them here with minimal overhead:
        # if STATE.engine_status["mom_bull"] == "1":
        #     await momentum_engine.on_tick(tick)
        # etc.


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
    # data_connected flags for UI dots
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
    # UI expects dict keyed by side with list of trades/orders
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

    fn = getattr(TradeControl, "save_engine_settings", None)
    if callable(fn):
        await fn(side, payload)
        return {"status": "success"}

    # fallback direct redis
    r = await _get_redis_client()
    await r.set(f"nexus:settings:{side}", json.dumps(payload))
    return {"status": "success"}


@app.get("/api/settings/engine/{side}")
async def get_engine_settings(side: str):
    if side not in ENGINE_SIDES:
        raise HTTPException(status_code=400, detail="Invalid side")

    fn = getattr(TradeControl, "get_engine_settings", None)
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
        enabled = bool(payload.get("enabled"))
        if side not in ENGINE_SIDES:
            raise HTTPException(status_code=400, detail="Invalid side")
        await _save_engine_status(side, enabled)
        return {"status": "success"}

    if action == "save_api":
        # from UI: broker=dhan, client_id, access_token
        client_id = (payload.get("client_id") or "").strip()
        access_token = (payload.get("access_token") or "").strip()

        save_fn = getattr(TradeControl, "save_config", None)
        if callable(save_fn):
            await save_fn(client_id, access_token)
        else:
            # fallback direct redis keys
            r = await _get_redis_client()
            await r.set("nexus:config:client_id", client_id)
            await r.set("nexus:config:access_token", access_token)

        return {"status": "success"}

    if action == "square_off_one":
        # placeholder: UI just wants it to succeed
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
    try:
        # touch redis via TradeControl (your module likely logs "Redis connected successfully.")
        _ = await TradeControl.get_access_token()
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

    # best-effort close websocket (prevents keepalive pending warning)
    try:
        if STATE._ws is not None:
            await STATE._ws.send(json.dumps({"RequestCode": 12}))
            await STATE._ws.close()
    except Exception:
        pass

    logger.info("Shutdown complete")
