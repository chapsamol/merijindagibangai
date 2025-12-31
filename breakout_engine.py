# # -*- coding: utf-8 -*-
# import asyncio
# import logging
# from datetime import datetime
# from math import floor
# from typing import Optional
# import pytz

# from redis_manager import TradeControl

# logger = logging.getLogger("Nexus_Breakout")
# IST = pytz.timezone("Asia/Kolkata")


# class BreakoutEngine:
#     """
#     Breakout Engine (Bull/Bear) — Dhan Orders

#     - run() is tick-fast
#     - on_candle_close() is called by main.py candle worker
#     - per-symbol lock is handled outside (main.py token_locks)
#     - Redis reservations unchanged
#     - Orders via dhan.place_order(...)
#     - token == security_id
#     - Target/Trailing are recomputed from executed AvgTradedPrice via OrderUpdate in main.py
#     """

#     EXIT_BUFFER_PCT = 0.0001
#     TRIGGER_VALID_SECONDS = 6 * 60
#     MAX_TRADES_PER_SYMBOL = 2

#     # -----------------------------
#     # TICK FAST-PATH
#     # -----------------------------
#     @staticmethod
#     async def run(token: int, ltp: float, vol: int, state: dict):
#         stock = state["stocks"].get(token)
#         if not stock:
#             return

#         symbol = stock.get("symbol") or ""
#         if not symbol:
#             return

#         stock["ltp"] = float(ltp or 0.0)
#         brk_status = (stock.get("brk_status") or "WAITING").upper()

#         # Always monitor open trade even if engine toggle off
#         if brk_status == "OPEN":
#             await BreakoutEngine.monitor_active_trade(stock, float(ltp), state)
#             return

#         if brk_status != "TRIGGER_WATCH":
#             return

#         side = (stock.get("brk_side_latch") or "").lower()
#         if side not in ("bull", "bear"):
#             logger.warning(f"[BRK] {symbol} invalid side_latch; resetting.")
#             BreakoutEngine._reset_waiting(stock)
#             return

#         # TTL check
#         now_ts = int(datetime.now(IST).timestamp())
#         set_ts = int(stock.get("brk_trigger_set_ts") or 0)
#         if set_ts and (now_ts - set_ts) > BreakoutEngine.TRIGGER_VALID_SECONDS:
#             logger.info(f"⏳ [BRK-EXPIRE] {symbol} {side.upper()} trigger expired (>6m). Reset.")
#             BreakoutEngine._reset_waiting(stock)
#             return

#         # Engine toggle gates new entry only
#         if not bool(state["engine_live"].get(side, True)):
#             return

#         if not BreakoutEngine._within_trade_window(state["config"].get(side, {})):
#             logger.info(f"🕒 [BRK-WINDOW] {symbol} {side.upper()} outside trade window; reset.")
#             BreakoutEngine._reset_waiting(stock)
#             return

#         trig = float(stock.get("brk_trigger_px", 0.0) or 0.0)
#         if trig <= 0:
#             BreakoutEngine._reset_waiting(stock)
#             return

#         px = float(ltp)
#         if side == "bull" and px > trig:
#             logger.info(f"⚡ [BRK-TRIGGER] {symbol} BULL ltp {px:.2f} > {trig:.2f}")
#             await BreakoutEngine.open_trade(stock, px, state, "bull")
#         elif side == "bear" and px < trig:
#             logger.info(f"⚡ [BRK-TRIGGER] {symbol} BEAR ltp {px:.2f} < {trig:.2f}")
#             await BreakoutEngine.open_trade(stock, px, state, "bear")

#     # -----------------------------
#     # CANDLE CLOSE QUALIFICATION
#     # -----------------------------
#     @staticmethod
#     async def on_candle_close(token: int, candle: dict, state: dict):
#         stock = state["stocks"].get(token)
#         if not stock:
#             return

#         symbol = stock.get("symbol") or ""
#         if not symbol:
#             return

#         if (stock.get("brk_status") or "WAITING").upper() in ("OPEN", "TRIGGER_WATCH"):
#             return

#         # per-symbol cap check
#         try:
#             taken = await TradeControl.get_symbol_trade_count(symbol)
#             if int(taken) >= BreakoutEngine.MAX_TRADES_PER_SYMBOL:
#                 return
#         except Exception as e:
#             logger.warning(f"[BRK] {symbol} trade_count check failed: {e}")

#         pdh = float(stock.get("pdh", 0) or 0)
#         pdl = float(stock.get("pdl", 0) or 0)
#         if pdh <= 0 or pdl <= 0:
#             return

#         c_open = float(candle.get("open", 0))
#         high = float(candle.get("high", 0) or 0)
#         low = float(candle.get("low", 0) or 0)
#         close = float(candle.get("close", 0) or 0)
#         c_vol = int(candle.get("volume", 0) or 0)
#         if close <= 0 or high <= 0 or low <= 0:
#             return

#         now = datetime.now(IST)

#         # --- BULL BREAKOUT ---
#         if close > pdh and c_open < pdh:
#             side = "bull"
#             if not bool(state["engine_live"].get(side, True)):
#                 return
#             if not BreakoutEngine._within_trade_window(state["config"].get(side, {}), now=now):
#                 return
#             if not BreakoutEngine._range_gate_ok(side, high, low, close, pdh=pdh, pdl=pdl):
#                 return
#             ok, detail = await BreakoutEngine.check_vol_matrix(stock, candle, side, state)
#             if not ok:
#                 return

#             stock["brk_status"] = "TRIGGER_WATCH"
#             stock["brk_side_latch"] = "bull"
#             stock["brk_trigger_px"] = float(high)
#             stock["brk_trigger_set_ts"] = int(now.timestamp())
#             stock["brk_trigger_candle"] = dict(candle)

#             stock["brk_scan_vol"] = int(c_vol)
#             stock["brk_scan_reason"] = f"PDH break + Vol OK ({detail})"
#             stock["brk_scan_seen_ts"] = None
#             stock["brk_scan_seen_time"] = None
#             logger.info(f"✅ [BRK-QUALIFIED] {symbol} BULL trigger@{high:.2f} {detail}")
#             return

#         # --- BEAR BREAKDOWN ---
#         if close < pdl and c_open > pdl:
#             side = "bear"
#             if not bool(state["engine_live"].get(side, True)):
#                 return
#             if not BreakoutEngine._within_trade_window(state["config"].get(side, {}), now=now):
#                 return
#             if not BreakoutEngine._range_gate_ok(side, high, low, close, pdh=pdh, pdl=pdl):
#                 return
#             ok, detail = await BreakoutEngine.check_vol_matrix(stock, candle, side, state)
#             if not ok:
#                 return

#             stock["brk_status"] = "TRIGGER_WATCH"
#             stock["brk_side_latch"] = "bear"
#             stock["brk_trigger_px"] = float(low)
#             stock["brk_trigger_set_ts"] = int(now.timestamp())
#             stock["brk_trigger_candle"] = dict(candle)

#             stock["brk_scan_vol"] = int(c_vol)
#             stock["brk_scan_reason"] = f"PDL break + Vol OK ({detail})"
#             stock["brk_scan_seen_ts"] = None
#             stock["brk_scan_seen_time"] = None
#             logger.info(f"✅ [BRK-QUALIFIED] {symbol} BEAR trigger@{low:.2f} {detail}")
#             return

#     @staticmethod
#     async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
#         cfg = state["config"].get(side, {})
#         matrix = cfg.get("volume_criteria", []) or []

#         c_vol = int(candle.get("volume", 0) or 0)
#         s_sma = float(stock.get("sma", 0) or 0)
#         close = float(candle.get("close", 0) or 0)
#         turnover_cr = (c_vol * close) / 10000000.0 if close > 0 else 0.0

#         if not matrix:
#             return True, "NoMatrix"

#         best_fail = None
#         for i, level in enumerate(matrix):
#             try:
#                 min_sma_avg = float(level.get("min_sma_avg", 0) or 0)
#                 sma_mult = float(level.get("sma_multiplier", 1.0) or 1.0)
#                 min_cr = float(level.get("min_vol_price_cr", 0) or 0)
#             except Exception:
#                 continue

#             if s_sma < min_sma_avg:
#                 best_fail = best_fail or f"L{i+1} skip (SMA<{min_sma_avg})"
#                 continue

#             required_vol = s_sma * sma_mult
#             if (c_vol >= required_vol) and (turnover_cr >= min_cr):
#                 return True, f"L{i+1} Pass (OR)"

#             best_fail = f"L{i+1} Fail (vol {c_vol}<{required_vol:.0f} or cr {turnover_cr:.2f}<{min_cr})"

#         return False, best_fail or "NoRuleMatched"

#     # -----------------------------
#     # OPEN TRADE (Dhan)
#     # -----------------------------
#     @staticmethod
#     async def open_trade(stock: dict, ltp: float, state: dict, side_key: str):
#         symbol = stock.get("symbol") or ""
#         if not symbol:
#             BreakoutEngine._reset_waiting(stock)
#             return

#         side_key = side_key.lower().strip()
#         if side_key not in ("bull", "bear"):
#             BreakoutEngine._reset_waiting(stock)
#             return

#         cfg = state["config"].get(side_key, {}) or {}
#         dhan = state.get("dhan")
#         if not dhan:
#             logger.error(f"❌ [BRK] {symbol} dhan session missing")
#             BreakoutEngine._reset_waiting(stock)
#             return

#         if not bool(state["engine_live"].get(side_key, True)):
#             BreakoutEngine._reset_waiting(stock)
#             return

#         if not BreakoutEngine._within_trade_window(cfg):
#             BreakoutEngine._reset_waiting(stock)
#             return

#         txn_type = dhan.BUY if side_key == "bull" else dhan.SELL

#         # -------------------- reservations --------------------
#         side_limit = int(cfg.get("total_trades", 5) or 5)

#         if not await TradeControl.reserve_side_trade(side_key, side_limit):
#             BreakoutEngine._reset_waiting(stock)
#             return

#         ok, _reason = await TradeControl.reserve_symbol_trade(symbol, max_trades=BreakoutEngine.MAX_TRADES_PER_SYMBOL)
#         if not ok:
#             await TradeControl.rollback_side_trade(side_key)
#             BreakoutEngine._reset_waiting(stock)
#             return

#         # -------------------- SL derived from trigger candle size --------------------
#         trig_candle = stock.get("brk_trigger_candle") or {}
#         entry = float(ltp)

#         tc_high = float(trig_candle.get("high", 0) or 0)
#         tc_low = float(trig_candle.get("low", 0) or 0)
#         tc_close = float(trig_candle.get("close", 0) or 0) or entry

#         range_pct = ((tc_high - tc_low) / tc_close) * 100.0 if tc_close > 0 else 0.0
#         pdh = float(stock.get("pdh", 0) or 0)
#         pdl = float(stock.get("pdl", 0) or 0)

#         if range_pct <= 0.5:
#             sl_px = tc_low if side_key == "bull" else tc_high
#         else:
#             sl_px = 0.0
#             if side_key == "bull" and pdh > 0 and pdh < entry:
#                 sl_px = pdh
#             elif side_key == "bear" and pdl > 0 and pdl > entry:
#                 sl_px = pdl
#             if sl_px <= 0:
#                 sl_px = tc_low if side_key == "bull" else tc_high
#             if sl_px <= 0:
#                 sl_px = round(entry * (0.995 if side_key == "bull" else 1.005), 2)

#         risk_per_share = abs(entry - sl_px)
#         risk_amount = float(cfg.get("risk_trade_1", 2000) or 2000)
#         qty = floor(risk_amount / risk_per_share) if risk_per_share > 0 else 0

#         if qty <= 0:
#             await TradeControl.rollback_symbol_trade(symbol)
#             await TradeControl.rollback_side_trade(side_key)
#             BreakoutEngine._reset_waiting(stock)
#             return

#         # ratios for executed-price recompute in main.py
#         try:
#             rr_val = float(str(cfg.get("risk_reward", "1:2")).split(":")[-1])
#         except Exception:
#             rr_val = 2.0

#         try:
#             tsl_ratio = float(str(cfg.get("trailing_sl", "1:1.5")).split(":")[-1])
#         except Exception:
#             tsl_ratio = 1.5

#         target = round(entry + (risk_per_share * rr_val), 2) if side_key == "bull" else round(entry - (risk_per_share * rr_val), 2)
#         trail_step = float(risk_per_share * tsl_ratio) if tsl_ratio > 0 else float(risk_per_share)

#         security_id = str(int(stock.get("security_id") or stock.get("token") or 0))
#         if security_id == "0":
#             await TradeControl.rollback_symbol_trade(symbol)
#             await TradeControl.rollback_side_trade(side_key)
#             BreakoutEngine._reset_waiting(stock)
#             return

#         try:
#             logger.info(
#                 f"🧾 [BRK-ORDER] {symbol} {side_key.upper()} "
#                 f"secId={security_id} qty={qty} entry(live)={entry:.2f} sl={sl_px:.2f} tgt={target:.2f}"
#             )

#             resp = await asyncio.to_thread(
#                 dhan.place_order,
#                 security_id=security_id,
#                 exchange_segment=dhan.NSE,
#                 transaction_type=txn_type,
#                 quantity=int(qty),
#                 order_type=dhan.MARKET,
#                 product_type=dhan.INTRA,
#                 price=0
#             )

#             order_id = BreakoutEngine._extract_dhan_order_id(resp)
#             if not order_id:
#                 raise RuntimeError(f"OrderId missing in response: {resp}")

#             trade = {
#                 "engine": "breakout",
#                 "side": side_key,
#                 "symbol": symbol,
#                 "security_id": int(security_id),

#                 "qty": int(qty),

#                 "entry_price": float(entry),     # overwritten to executed later
#                 "entry_exec_price": None,

#                 "sl_price": float(sl_px),
#                 "target_price": float(target),

#                 "order_id": str(order_id),
#                 "exit_order_id": None,

#                 "pnl": 0.0,
#                 "status": "OPEN",
#                 "entry_time": datetime.now(IST).strftime("%H:%M:%S"),

#                 "init_risk": float(risk_per_share),
#                 "trail_step": float(trail_step),

#                 "rr_val": float(rr_val),
#                 "tsl_ratio": float(tsl_ratio),
#             }

#             state["trades"][side_key].append(trade)

#             stock["brk_status"] = "OPEN"
#             stock["brk_active_trade"] = trade
#             stock["brk_side_latch"] = side_key

#             stock.pop("brk_trigger_px", None)
#             stock["brk_trigger_set_ts"] = None
#             stock["brk_scan_seen_ts"] = None
#             stock["brk_scan_seen_time"] = None

#             logger.info(f"🚀 [BRK-ENTRY] {symbol} {side_key.upper()} order={order_id} qty={qty}")

#         except Exception as e:
#             logger.error(f"❌ [BRK-ORDER-FAIL] {symbol}: {e}")
#             await TradeControl.rollback_symbol_trade(symbol)
#             await TradeControl.rollback_side_trade(side_key)
#             BreakoutEngine._reset_waiting(stock)

#     @staticmethod
#     def _extract_dhan_order_id(resp: object) -> Optional[str]:
#         if resp is None:
#             return None
#         if isinstance(resp, dict):
#             for k in ("orderId", "order_id", "orderNo", "order_no", "OrderId", "OrderNo"):
#                 if resp.get(k):
#                     return str(resp.get(k))
#             data = resp.get("data") if isinstance(resp.get("data"), dict) else None
#             if data:
#                 for k in ("orderId", "order_id", "orderNo", "order_no", "OrderId", "OrderNo"):
#                     if data.get(k):
#                         return str(data.get(k))
#         s = str(resp).strip()
#         return s if s else None

#     # -----------------------------
#     # MONITOR + EXIT
#     # -----------------------------
#     @staticmethod
#     async def monitor_active_trade(stock: dict, ltp: float, state: dict):
#         trade = stock.get("brk_active_trade")
#         if not trade:
#             return

#         symbol = stock.get("symbol") or ""
#         side_key = (stock.get("brk_side_latch") or "").lower()
#         if side_key not in ("bull", "bear"):
#             await BreakoutEngine.close_position(stock, state, "BAD_SIDE_LATCH")
#             return

#         is_bull = (side_key == "bull")

#         entry = float(trade.get("entry_price", 0) or 0)
#         qty = int(trade.get("qty", 0) or 0)
#         sl = float(trade.get("sl_price", 0) or 0)
#         target = float(trade.get("target_price", 0) or 0)

#         if entry <= 0 or qty <= 0:
#             await BreakoutEngine.close_position(stock, state, "BAD_TRADE_STATE")
#             return

#         trade["pnl"] = round(((float(ltp) - entry) * qty) if is_bull else ((entry - float(ltp)) * qty), 2)

#         b = BreakoutEngine.EXIT_BUFFER_PCT
#         if is_bull:
#             target_hit = float(ltp) >= (target * (1.0 - b))
#             sl_hit = float(ltp) <= (sl * (1.0 + b))
#         else:
#             target_hit = float(ltp) <= (target * (1.0 + b))
#             sl_hit = float(ltp) >= (sl * (1.0 - b))

#         if target_hit:
#             logger.info(f"🎯 [BRK-TARGET] {symbol} tgt={target:.2f} ltp={ltp:.2f} pnl={trade['pnl']}")
#             await BreakoutEngine.close_position(stock, state, "TARGET")
#             return

#         if sl_hit:
#             logger.info(f"🛑 [BRK-SL] {symbol} sl={sl:.2f} ltp={ltp:.2f} pnl={trade['pnl']}")
#             await BreakoutEngine.close_position(stock, state, "SL")
#             return

#         new_sl = BreakoutEngine._step_trail_sl(trade, float(ltp), is_bull)
#         if new_sl is not None:
#             cur = float(trade.get("sl_price", 0) or 0)
#             if is_bull and new_sl > cur:
#                 trade["sl_price"] = float(new_sl)
#             elif (not is_bull) and new_sl < cur:
#                 trade["sl_price"] = float(new_sl)

#         if symbol in state.get("manual_exits", set()):
#             await BreakoutEngine.close_position(stock, state, "MANUAL")
#             state["manual_exits"].remove(symbol)

#     @staticmethod
#     def _step_trail_sl(trade: dict, ltp: float, is_bull: bool) -> Optional[float]:
#         entry = float(trade.get("entry_price", 0) or 0)
#         init_risk = float(trade.get("init_risk", 0) or 0)
#         step = float(trade.get("trail_step", 0) or 0)

#         if entry <= 0 or init_risk <= 0 or step <= 0:
#             return None

#         profit = (ltp - entry) if is_bull else (entry - ltp)
#         if profit <= 0:
#             return None

#         k = int(profit // step)
#         if k < 1:
#             return None

#         desired = entry + ((k - 1) * step) if is_bull else entry - ((k - 1) * step)
#         return round(desired, 2)

#     @staticmethod
#     async def close_position(stock: dict, state: dict, reason: str):
#         trade = stock.get("brk_active_trade")
#         dhan = state.get("dhan")
#         symbol = stock.get("symbol") or ""
#         side_key = (stock.get("brk_side_latch") or "").lower()

#         is_bull = (side_key == "bull")
#         txn_type = None
#         if dhan:
#             txn_type = dhan.SELL if is_bull else dhan.BUY

#         security_id = str(int(stock.get("security_id") or stock.get("token") or 0))

#         if trade and dhan and security_id != "0" and txn_type:
#             try:
#                 logger.info(f"🏁 [BRK-EXIT] {symbol} reason={reason} qty={trade.get('qty')}")
#                 resp = await asyncio.to_thread(
#                     dhan.place_order,
#                     security_id=security_id,
#                     exchange_segment=dhan.NSE,
#                     transaction_type=txn_type,
#                     quantity=int(trade["qty"]),
#                     order_type=dhan.MARKET,
#                     product_type=dhan.INTRA,
#                     price=0
#                 )
#                 exit_id = BreakoutEngine._extract_dhan_order_id(resp)
#                 trade["exit_order_id"] = str(exit_id) if exit_id else None
#             except Exception as e:
#                 logger.error(f"❌ [BRK-EXIT-FAIL] {symbol}: {e}")

#         if trade:
#             trade["status"] = "CLOSED"
#             trade["exit_time"] = datetime.now(IST).strftime("%H:%M:%S")
#             trade["exit_reason"] = reason

#         if symbol:
#             try:
#                 await TradeControl.release_symbol_lock(symbol)
#             except Exception as e:
#                 logger.warning(f"[BRK] {symbol} release lock failed: {e}")

#         BreakoutEngine._reset_waiting(stock)

#     # -----------------------------
#     # HELPERS
#     # -----------------------------
#     @staticmethod
#     def _reset_waiting(stock: dict):
#         stock["brk_status"] = "WAITING"
#         stock["brk_active_trade"] = None

#         stock.pop("brk_trigger_px", None)
#         stock.pop("brk_side_latch", None)
#         stock["brk_trigger_set_ts"] = None
#         stock.pop("brk_trigger_candle", None)

#         stock["brk_scan_seen_ts"] = None
#         stock["brk_scan_seen_time"] = None
#         stock["brk_scan_vol"] = 0
#         stock["brk_scan_reason"] = None

#     @staticmethod
#     def _within_trade_window(cfg: dict, now: Optional[datetime] = None) -> bool:
#         from datetime import time as dtime
#         try:
#             now = now or datetime.now(IST)
#             start_s = str(cfg.get("trade_start", "09:15"))
#             end_s = str(cfg.get("trade_end", "15:10"))
#             sh, sm = map(int, start_s.split(":"))
#             eh, em = map(int, end_s.split(":"))
#             start_t = dtime(sh, sm)
#             end_t = dtime(eh, em)
#             nt = now.time()
#             return (nt >= start_t) and (nt <= end_t)
#         except Exception:
#             return True

#     @staticmethod
#     def _range_gate_ok(side: str, high: float, low: float, close: float, *, pdh: float, pdl: float) -> bool:
#         if close <= 0:
#             return False
#         range_pct = ((high - low) / close) * 100.0
#         if range_pct <= 0.5:
#             return True
#         if side == "bull":
#             if pdh <= 0:
#                 return False
#             gap_pct = ((high - pdh) / pdh) * 100.0
#             return gap_pct <= 0.5
#         if side == "bear":
#             if pdl <= 0:
#                 return False
#             gap_pct = ((pdl - low) / pdl) * 100.0
#             return gap_pct <= 0.5
#         return False

# -*- coding: utf-8 -*-
import asyncio
import logging
import time
from datetime import datetime
from math import floor
from typing import Optional, Tuple, Any, Dict
from queue import SimpleQueue
from threading import Thread
import pytz

from redis_manager import TradeControl

logger = logging.getLogger("Nexus_Breakout")
IST = pytz.timezone("Asia/Kolkata")


class _OrderWorker:
    """
    Persistent single thread for synchronous dhan.place_order calls.
    Avoids asyncio.to_thread() overhead + threadpool jitter.
    """
    def __init__(self) -> None:
        self._q: SimpleQueue[Tuple[asyncio.AbstractEventLoop, asyncio.Future, Any, Dict[str, Any]]] = SimpleQueue()
        self._t = Thread(target=self._run, name="dhan_order_worker", daemon=True)
        self._t.start()

    def submit(self, func: Any, **kwargs: Any) -> "asyncio.Future":
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._q.put((loop, fut, func, kwargs))
        return fut

    def _run(self) -> None:
        while True:
            loop, fut, func, kwargs = self._q.get()
            try:
                res = func(**kwargs)
                loop.call_soon_threadsafe(fut.set_result, res)
            except Exception as e:
                loop.call_soon_threadsafe(fut.set_exception, e)


class BreakoutEngine:
    """
    Breakout Engine (Bull/Bear) — Dhan Orders

    - run() is tick-fast
    - on_candle_close() is called by main.py candle worker
    - Redis reservations unchanged, but moved off the trigger hot-path
    - Orders via dhan.place_order(...) using a persistent worker thread
    - token == security_id
    - Target/Trailing recomputed from executed AvgTradedPrice via OrderUpdate in main.py
    """

    EXIT_BUFFER_PCT = 0.0001
    TRIGGER_VALID_SECONDS = 6 * 60
    MAX_TRADES_PER_SYMBOL = 2

    # cache for trade window parsing: ("09:15","15:10") -> (start_time, end_time)
    _tw_cache: Dict[Tuple[str, str], Tuple[Any, Any]] = {}

    # persistent order thread
    _order_worker: Optional[_OrderWorker] = None

    @classmethod
    def _get_order_worker(cls) -> _OrderWorker:
        ow = cls._order_worker
        if ow is None:
            ow = _OrderWorker()
            cls._order_worker = ow
        return ow

    # -----------------------------
    # TICK FAST-PATH
    # -----------------------------
    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        stock = state["stocks"].get(token)
        if not stock:
            return

        symbol = stock.get("symbol") or ""
        if not symbol:
            return

        px = float(ltp or 0.0)
        stock["ltp"] = px

        brk_status = (stock.get("brk_status") or "WAITING").upper()

        # Always monitor open trade even if engine toggle off
        if brk_status == "OPEN":
            await BreakoutEngine.monitor_active_trade(stock, px, state)
            return

        if brk_status != "TRIGGER_WATCH":
            return

        side = (stock.get("brk_side_latch") or "").lower()
        if side not in ("bull", "bear"):
            # rollback reservations if any
            await BreakoutEngine._rollback_watch_reservations(stock)
            BreakoutEngine._reset_waiting(stock)
            return

        # TTL check (use time.time() not datetime conversion per tick)
        now_ts = int(time.time())
        set_ts = int(stock.get("brk_trigger_set_ts") or 0)
        if set_ts and (now_ts - set_ts) > BreakoutEngine.TRIGGER_VALID_SECONDS:
            # Trigger expired: rollback held reservations and reset
            await BreakoutEngine._rollback_watch_reservations(stock)
            BreakoutEngine._reset_waiting(stock)
            return

        # Engine toggle gates new entry only
        if not bool(state["engine_live"].get(side, True)):
            return

        # Trade window check (cached parsing)
        if not BreakoutEngine._within_trade_window(state["config"].get(side, {})):
            await BreakoutEngine._rollback_watch_reservations(stock)
            BreakoutEngine._reset_waiting(stock)
            return

        trig = float(stock.get("brk_trigger_px", 0.0) or 0.0)
        if trig <= 0.0:
            await BreakoutEngine._rollback_watch_reservations(stock)
            BreakoutEngine._reset_waiting(stock)
            return

        # Trigger checks (no await before open_trade)
        if side == "bull":
            if px > trig:
                await BreakoutEngine.open_trade(stock, px, state, "bull")
        else:
            if px < trig:
                await BreakoutEngine.open_trade(stock, px, state, "bear")

    # -----------------------------
    # CANDLE CLOSE QUALIFICATION
    # -----------------------------
    @staticmethod
    async def on_candle_close(token: int, candle: dict, state: dict):
        stock = state["stocks"].get(token)
        if not stock:
            return

        symbol = stock.get("symbol") or ""
        if not symbol:
            return

        status = (stock.get("brk_status") or "WAITING").upper()
        if status in ("OPEN", "TRIGGER_WATCH"):
            return

        # per-symbol cap check (kept as-is; candle-close is not hot-path)
        try:
            taken = await TradeControl.get_symbol_trade_count(symbol)
            if int(taken) >= BreakoutEngine.MAX_TRADES_PER_SYMBOL:
                return
        except Exception as e:
            logger.warning(f"[BRK] {symbol} trade_count check failed: {e}")

        pdh = float(stock.get("pdh", 0) or 0.0)
        pdl = float(stock.get("pdl", 0) or 0.0)
        if pdh <= 0.0 or pdl <= 0.0:
            return

        c_open = float(candle.get("open", 0) or 0.0)
        high = float(candle.get("high", 0) or 0.0)
        low = float(candle.get("low", 0) or 0.0)
        close = float(candle.get("close", 0) or 0.0)
        c_vol = int(candle.get("volume", 0) or 0)
        if close <= 0.0 or high <= 0.0 or low <= 0.0:
            return

        now = datetime.now(IST)

        # ----- BULL BREAKOUT QUALIFY -----
        if close > pdh and c_open < pdh:
            side = "bull"
            if not bool(state["engine_live"].get(side, True)):
                return
            if not BreakoutEngine._within_trade_window(state["config"].get(side, {}), now=now):
                return
            if not BreakoutEngine._range_gate_ok(side, high, low, close, pdh=pdh, pdl=pdl):
                return
            ok, detail = await BreakoutEngine.check_vol_matrix(stock, candle, side, state)
            if not ok:
                return

            # PRECOMPUTE + RESERVE HERE (moves Redis awaits off trigger hot-path)
            reserved = await BreakoutEngine._reserve_for_watch(stock, state, side, symbol)
            if not reserved:
                return

            BreakoutEngine._set_trigger_watch(
                stock=stock,
                side=side,
                trigger_px=float(high),
                now_ts=int(now.timestamp()),
                candle=candle,
                scan_vol=c_vol,
                reason=f"PDH break + Vol OK ({detail})",
                pdh=pdh,
                pdl=pdl,
                close=close,
            )
            return

        # ----- BEAR BREAKDOWN QUALIFY -----
        if close < pdl and c_open > pdl:
            side = "bear"
            if not bool(state["engine_live"].get(side, True)):
                return
            if not BreakoutEngine._within_trade_window(state["config"].get(side, {}), now=now):
                return
            if not BreakoutEngine._range_gate_ok(side, high, low, close, pdh=pdh, pdl=pdl):
                return
            ok, detail = await BreakoutEngine.check_vol_matrix(stock, candle, side, state)
            if not ok:
                return

            reserved = await BreakoutEngine._reserve_for_watch(stock, state, side, symbol)
            if not reserved:
                return

            BreakoutEngine._set_trigger_watch(
                stock=stock,
                side=side,
                trigger_px=float(low),
                now_ts=int(now.timestamp()),
                candle=candle,
                scan_vol=c_vol,
                reason=f"PDL break + Vol OK ({detail})",
                pdh=pdh,
                pdl=pdl,
                close=close,
            )
            return

    @staticmethod
    def _set_trigger_watch(
        *,
        stock: dict,
        side: str,
        trigger_px: float,
        now_ts: int,
        candle: dict,
        scan_vol: int,
        reason: str,
        pdh: float,
        pdl: float,
        close: float,
    ) -> None:
        """
        Sets TRIGGER_WATCH and stores precomputed values for low-latency open_trade().
        """
        stock["brk_status"] = "TRIGGER_WATCH"
        stock["brk_side_latch"] = side
        stock["brk_trigger_px"] = float(trigger_px)
        stock["brk_trigger_set_ts"] = int(now_ts)
        stock["brk_trigger_candle"] = dict(candle)

        stock["brk_scan_vol"] = int(scan_vol)
        stock["brk_scan_reason"] = reason
        stock["brk_scan_seen_ts"] = None
        stock["brk_scan_seen_time"] = None

        # Precompute SL anchor from trigger candle + pdh/pdl logic (same logic as open_trade did)
        tc_high = float(candle.get("high", 0) or 0.0)
        tc_low = float(candle.get("low", 0) or 0.0)
        tc_close = float(candle.get("close", 0) or 0.0) or float(close)

        range_pct = ((tc_high - tc_low) / tc_close) * 100.0 if tc_close > 0 else 0.0

        if range_pct <= 0.5:
            sl_px = tc_low if side == "bull" else tc_high
        else:
            sl_px = 0.0
            # entry unknown at candle close; we keep “anchor” rules same, fallback later if needed
            if side == "bull" and pdh > 0:
                sl_px = pdh
            elif side == "bear" and pdl > 0:
                sl_px = pdl
            if sl_px <= 0.0:
                sl_px = tc_low if side == "bull" else tc_high

        stock["brk_pre_sl_px"] = float(sl_px) if sl_px > 0 else 0.0

        # Precompute RR/TSL parsing once
        # (keeps same parsing behavior as before)
        cfg = (stock.get("brk_pre_cfg") or {})  # may be set by _reserve_for_watch
        try:
            rr_val = float(str(cfg.get("risk_reward", "1:2")).split(":")[-1])
        except Exception:
            rr_val = 2.0
        try:
            tsl_ratio = float(str(cfg.get("trailing_sl", "1:1.5")).split(":")[-1])
        except Exception:
            tsl_ratio = 1.5

        stock["brk_pre_rr_val"] = float(rr_val)
        stock["brk_pre_tsl_ratio"] = float(tsl_ratio)

        # Precompute risk amount + side limit
        stock["brk_pre_risk_amount"] = float(cfg.get("risk_trade_1", 2000) or 2000)
        stock["brk_pre_side_limit"] = int(cfg.get("total_trades", 5) or 5)

    @staticmethod
    async def _reserve_for_watch(stock: dict, state: dict, side_key: str, symbol: str) -> bool:
        """
        Reserve side + symbol at candle close so trigger moment has no Redis await.
        Rollback happens on expiry/reset or order failure.
        """
        cfg = state["config"].get(side_key, {}) or {}
        stock["brk_pre_cfg"] = cfg  # used by precompute above

        side_limit = int(cfg.get("total_trades", 5) or 5)

        # If already reserved (shouldn't happen normally), don't double-reserve
        if bool(stock.get("brk_reserved_watch")):
            return True

        try:
            ok_side = await TradeControl.reserve_side_trade(side_key, side_limit)
            if not ok_side:
                return False

            ok_sym, _reason = await TradeControl.reserve_symbol_trade(symbol, max_trades=BreakoutEngine.MAX_TRADES_PER_SYMBOL)
            if not ok_sym:
                await TradeControl.rollback_side_trade(side_key)
                return False

            stock["brk_reserved_watch"] = True
            stock["brk_reserved_side"] = side_key
            stock["brk_reserved_symbol"] = symbol
            return True
        except Exception as e:
            # best-effort rollback if side reserved but symbol failed mid-way
            try:
                await TradeControl.rollback_side_trade(side_key)
            except Exception:
                pass
            return False

    @staticmethod
    async def _rollback_watch_reservations(stock: dict) -> None:
        if not bool(stock.get("brk_reserved_watch")):
            return
        side_key = (stock.get("brk_reserved_side") or "").lower()
        symbol = stock.get("brk_reserved_symbol") or ""
        try:
            if symbol:
                await TradeControl.rollback_symbol_trade(symbol)
        except Exception:
            pass
        try:
            if side_key in ("bull", "bear"):
                await TradeControl.rollback_side_trade(side_key)
        except Exception:
            pass

        stock["brk_reserved_watch"] = False
        stock.pop("brk_reserved_side", None)
        stock.pop("brk_reserved_symbol", None)

    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
        cfg = state["config"].get(side, {}) or {}
        matrix = cfg.get("volume_criteria", []) or []

        c_vol = int(candle.get("volume", 0) or 0)
        s_sma = float(stock.get("sma", 0) or 0.0)
        close = float(candle.get("close", 0) or 0.0)
        turnover_cr = (c_vol * close) / 10000000.0 if close > 0 else 0.0

        if not matrix:
            return True, "NoMatrix"

        best_fail = None
        for i, level in enumerate(matrix):
            try:
                min_sma_avg = float(level.get("min_sma_avg", 0) or 0.0)
                sma_mult = float(level.get("sma_multiplier", 1.0) or 1.0)
                min_cr = float(level.get("min_vol_price_cr", 0) or 0.0)
            except Exception:
                continue

            if s_sma < min_sma_avg:
                best_fail = best_fail or f"L{i+1} skip (SMA<{min_sma_avg})"
                continue

            required_vol = s_sma * sma_mult
            if (c_vol >= required_vol) and (turnover_cr >= min_cr):
                return True, f"L{i+1} Pass (OR)"

            best_fail = f"L{i+1} Fail (vol {c_vol}<{required_vol:.0f} or cr {turnover_cr:.2f}<{min_cr})"

        return False, best_fail or "NoRuleMatched"

    # -----------------------------
    # OPEN TRADE (Dhan)
    # -----------------------------
    @staticmethod
    async def open_trade(stock: dict, ltp: float, state: dict, side_key: str):
        symbol = stock.get("symbol") or ""
        if not symbol:
            await BreakoutEngine._rollback_watch_reservations(stock)
            BreakoutEngine._reset_waiting(stock)
            return

        side_key = side_key.lower().strip()
        if side_key not in ("bull", "bear"):
            await BreakoutEngine._rollback_watch_reservations(stock)
            BreakoutEngine._reset_waiting(stock)
            return

        # Gate again (cheap)
        if not bool(state["engine_live"].get(side_key, True)):
            await BreakoutEngine._rollback_watch_reservations(stock)
            BreakoutEngine._reset_waiting(stock)
            return

        cfg = state["config"].get(side_key, {}) or {}
        if not BreakoutEngine._within_trade_window(cfg):
            await BreakoutEngine._rollback_watch_reservations(stock)
            BreakoutEngine._reset_waiting(stock)
            return

        dhan = state.get("dhan")
        if not dhan:
            logger.error(f"❌ [BRK] {symbol} dhan session missing")
            await BreakoutEngine._rollback_watch_reservations(stock)
            BreakoutEngine._reset_waiting(stock)
            return

        # If watch reservations were not taken at candle close (older flow), take them here (fallback)
        if not bool(stock.get("brk_reserved_watch")):
            reserved = await BreakoutEngine._reserve_for_watch(stock, state, side_key, symbol)
            if not reserved:
                BreakoutEngine._reset_waiting(stock)
                return

        txn_type = dhan.BUY if side_key == "bull" else dhan.SELL

        # PRECOMPUTED values from candle close
        trig_candle = stock.get("brk_trigger_candle") or {}
        entry = float(ltp or 0.0)

        tc_high = float(trig_candle.get("high", 0) or 0.0)
        tc_low = float(trig_candle.get("low", 0) or 0.0)
        tc_close = float(trig_candle.get("close", 0) or 0.0) or entry

        # Use precomputed SL anchor but keep the same fallbacks as before
        sl_px = float(stock.get("brk_pre_sl_px", 0.0) or 0.0)
        if sl_px <= 0.0:
            # fallback to original logic
            range_pct = ((tc_high - tc_low) / tc_close) * 100.0 if tc_close > 0 else 0.0
            pdh = float(stock.get("pdh", 0) or 0.0)
            pdl = float(stock.get("pdl", 0) or 0.0)
            if range_pct <= 0.5:
                sl_px = tc_low if side_key == "bull" else tc_high
            else:
                sl_px = 0.0
                if side_key == "bull" and pdh > 0 and pdh < entry:
                    sl_px = pdh
                elif side_key == "bear" and pdl > 0 and pdl > entry:
                    sl_px = pdl
                if sl_px <= 0:
                    sl_px = tc_low if side_key == "bull" else tc_high
                if sl_px <= 0:
                    sl_px = round(entry * (0.995 if side_key == "bull" else 1.005), 2)

        risk_per_share = abs(entry - sl_px)
        risk_amount = float(stock.get("brk_pre_risk_amount", cfg.get("risk_trade_1", 2000)) or 2000.0)
        qty = floor(risk_amount / risk_per_share) if risk_per_share > 0 else 0

        if qty <= 0:
            await BreakoutEngine._rollback_watch_reservations(stock)
            BreakoutEngine._reset_waiting(stock)
            return

        rr_val = float(stock.get("brk_pre_rr_val", 2.0) or 2.0)
        tsl_ratio = float(stock.get("brk_pre_tsl_ratio", 1.5) or 1.5)

        target = round(entry + (risk_per_share * rr_val), 2) if side_key == "bull" else round(entry - (risk_per_share * rr_val), 2)
        trail_step = float(risk_per_share * tsl_ratio) if tsl_ratio > 0 else float(risk_per_share)

        security_id = str(int(stock.get("security_id") or stock.get("token") or 0))
        if security_id == "0":
            await BreakoutEngine._rollback_watch_reservations(stock)
            BreakoutEngine._reset_waiting(stock)
            return

        try:
            resp = await BreakoutEngine._place_order(
                dhan,
                security_id=security_id,
                exchange_segment=dhan.NSE,
                transaction_type=txn_type,
                quantity=int(qty),
                order_type=dhan.MARKET,
                product_type=dhan.INTRA,
                price=0,
            )

            order_id = BreakoutEngine._extract_dhan_order_id(resp)
            if not order_id:
                raise RuntimeError(f"OrderId missing in response: {resp}")

            trade = {
                "engine": "breakout",
                "side": side_key,
                "symbol": symbol,
                "security_id": int(security_id),

                "qty": int(qty),

                "entry_price": float(entry),     # overwritten to executed later
                "entry_exec_price": None,

                "sl_price": float(sl_px),
                "target_price": float(target),

                "order_id": str(order_id),
                "exit_order_id": None,

                "pnl": 0.0,
                "status": "OPEN",
                "entry_time": datetime.now(IST).strftime("%H:%M:%S"),

                "init_risk": float(risk_per_share),
                "trail_step": float(trail_step),

                "rr_val": float(rr_val),
                "tsl_ratio": float(tsl_ratio),
            }

            state["trades"][side_key].append(trade)

            stock["brk_status"] = "OPEN"
            stock["brk_active_trade"] = trade
            stock["brk_side_latch"] = side_key

            # Clear trigger + watch-only caches
            stock.pop("brk_trigger_px", None)
            stock["brk_trigger_set_ts"] = None
            stock["brk_scan_seen_ts"] = None
            stock["brk_scan_seen_time"] = None

            stock.pop("brk_pre_cfg", None)
            stock.pop("brk_pre_sl_px", None)
            stock.pop("brk_pre_rr_val", None)
            stock.pop("brk_pre_tsl_ratio", None)
            stock.pop("brk_pre_risk_amount", None)
            stock.pop("brk_pre_side_limit", None)

            # We consumed the reservation now; don't rollback it later
            stock["brk_reserved_watch"] = False
            stock.pop("brk_reserved_side", None)
            stock.pop("brk_reserved_symbol", None)

        except Exception as e:
            logger.error(f"❌ [BRK-ORDER-FAIL] {symbol}: {e}")
            await BreakoutEngine._rollback_watch_reservations(stock)
            BreakoutEngine._reset_waiting(stock)

    @staticmethod
    async def _place_order(dhan: Any, **kwargs: Any) -> Any:
        """
        Place order using persistent worker thread.
        """
        worker = BreakoutEngine._get_order_worker()
        fut = worker.submit(dhan.place_order, **kwargs)
        return await fut

    @staticmethod
    def _extract_dhan_order_id(resp: object) -> Optional[str]:
        if resp is None:
            return None
        if isinstance(resp, dict):
            for k in ("orderId", "order_id", "orderNo", "order_no", "OrderId", "OrderNo"):
                if resp.get(k):
                    return str(resp.get(k))
            data = resp.get("data") if isinstance(resp.get("data"), dict) else None
            if data:
                for k in ("orderId", "order_id", "orderNo", "order_no", "OrderId", "OrderNo"):
                    if data.get(k):
                        return str(data.get(k))
        s = str(resp).strip()
        return s if s else None

    # -----------------------------
    # MONITOR + EXIT
    # -----------------------------
    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        trade = stock.get("brk_active_trade")
        if not trade:
            return

        symbol = stock.get("symbol") or ""
        side_key = (stock.get("brk_side_latch") or "").lower()
        if side_key not in ("bull", "bear"):
            await BreakoutEngine.close_position(stock, state, "BAD_SIDE_LATCH")
            return

        is_bull = (side_key == "bull")

        entry = float(trade.get("entry_price", 0) or 0)
        qty = int(trade.get("qty", 0) or 0)
        sl = float(trade.get("sl_price", 0) or 0)
        target = float(trade.get("target_price", 0) or 0)

        if entry <= 0 or qty <= 0:
            await BreakoutEngine.close_position(stock, state, "BAD_TRADE_STATE")
            return

        trade["pnl"] = round(((float(ltp) - entry) * qty) if is_bull else ((entry - float(ltp)) * qty), 2)

        b = BreakoutEngine.EXIT_BUFFER_PCT
        if is_bull:
            target_hit = float(ltp) >= (target * (1.0 - b))
            sl_hit = float(ltp) <= (sl * (1.0 + b))
        else:
            target_hit = float(ltp) <= (target * (1.0 + b))
            sl_hit = float(ltp) >= (sl * (1.0 - b))

        if target_hit:
            logger.info(f"🎯 [BRK-TARGET] {symbol} tgt={target:.2f} ltp={ltp:.2f} pnl={trade['pnl']}")
            await BreakoutEngine.close_position(stock, state, "TARGET")
            return

        if sl_hit:
            logger.info(f"🛑 [BRK-SL] {symbol} sl={sl:.2f} ltp={ltp:.2f} pnl={trade['pnl']}")
            await BreakoutEngine.close_position(stock, state, "SL")
            return

        new_sl = BreakoutEngine._step_trail_sl(trade, float(ltp), is_bull)
        if new_sl is not None:
            cur = float(trade.get("sl_price", 0) or 0)
            if is_bull and new_sl > cur:
                trade["sl_price"] = float(new_sl)
            elif (not is_bull) and new_sl < cur:
                trade["sl_price"] = float(new_sl)

        if symbol in state.get("manual_exits", set()):
            await BreakoutEngine.close_position(stock, state, "MANUAL")
            state["manual_exits"].remove(symbol)

    @staticmethod
    def _step_trail_sl(trade: dict, ltp: float, is_bull: bool) -> Optional[float]:
        entry = float(trade.get("entry_price", 0) or 0)
        init_risk = float(trade.get("init_risk", 0) or 0)
        step = float(trade.get("trail_step", 0) or 0)

        if entry <= 0 or init_risk <= 0 or step <= 0:
            return None

        profit = (ltp - entry) if is_bull else (entry - ltp)
        if profit <= 0:
            return None

        k = int(profit // step)
        if k < 1:
            return None

        desired = entry + ((k - 1) * step) if is_bull else entry - ((k - 1) * step)
        return round(desired, 2)

    @staticmethod
    async def close_position(stock: dict, state: dict, reason: str):
        trade = stock.get("brk_active_trade")
        dhan = state.get("dhan")
        symbol = stock.get("symbol") or ""
        side_key = (stock.get("brk_side_latch") or "").lower()

        is_bull = (side_key == "bull")
        txn_type = None
        if dhan:
            txn_type = dhan.SELL if is_bull else dhan.BUY

        security_id = str(int(stock.get("security_id") or stock.get("token") or 0))

        if trade and dhan and security_id != "0" and txn_type:
            try:
                resp = await BreakoutEngine._place_order(
                    dhan,
                    security_id=security_id,
                    exchange_segment=dhan.NSE,
                    transaction_type=txn_type,
                    quantity=int(trade["qty"]),
                    order_type=dhan.MARKET,
                    product_type=dhan.INTRA,
                    price=0,
                )
                exit_id = BreakoutEngine._extract_dhan_order_id(resp)
                trade["exit_order_id"] = str(exit_id) if exit_id else None
            except Exception as e:
                logger.error(f"❌ [BRK-EXIT-FAIL] {symbol}: {e}")

        if trade:
            trade["status"] = "CLOSED"
            trade["exit_time"] = datetime.now(IST).strftime("%H:%M:%S")
            trade["exit_reason"] = reason

        if symbol:
            try:
                await TradeControl.release_symbol_lock(symbol)
            except Exception as e:
                logger.warning(f"[BRK] {symbol} release lock failed: {e}")

        BreakoutEngine._reset_waiting(stock)

    # -----------------------------
    # HELPERS
    # -----------------------------
    @staticmethod
    def _reset_waiting(stock: dict):
        stock["brk_status"] = "WAITING"
        stock["brk_active_trade"] = None

        stock.pop("brk_trigger_px", None)
        stock.pop("brk_side_latch", None)
        stock["brk_trigger_set_ts"] = None
        stock.pop("brk_trigger_candle", None)

        stock["brk_scan_seen_ts"] = None
        stock["brk_scan_seen_time"] = None
        stock["brk_scan_vol"] = 0
        stock["brk_scan_reason"] = None

        # clear watch reservations flags (actual rollback done by caller async)
        stock.pop("brk_reserved_watch", None)
        stock.pop("brk_reserved_side", None)
        stock.pop("brk_reserved_symbol", None)

        # clear precomputed keys
        stock.pop("brk_pre_cfg", None)
        stock.pop("brk_pre_sl_px", None)
        stock.pop("brk_pre_rr_val", None)
        stock.pop("brk_pre_tsl_ratio", None)
        stock.pop("brk_pre_risk_amount", None)
        stock.pop("brk_pre_side_limit", None)

    @staticmethod
    def _within_trade_window(cfg: dict, now: Optional[datetime] = None) -> bool:
        """
        Cached parsing of start/end times to avoid repeated split/int conversion.
        """
        from datetime import time as dtime
        try:
            now = now or datetime.now(IST)
            start_s = str(cfg.get("trade_start", "09:15"))
            end_s = str(cfg.get("trade_end", "15:10"))
            key = (start_s, end_s)

            cached = BreakoutEngine._tw_cache.get(key)
            if cached is None:
                sh, sm = map(int, start_s.split(":"))
                eh, em = map(int, end_s.split(":"))
                cached = (dtime(sh, sm), dtime(eh, em))
                BreakoutEngine._tw_cache[key] = cached

            start_t, end_t = cached
            nt = now.time()
            return (nt >= start_t) and (nt <= end_t)
        except Exception:
            return True

    @staticmethod
    def _range_gate_ok(side: str, high: float, low: float, close: float, *, pdh: float, pdl: float) -> bool:
        if close <= 0:
            return False
        range_pct = ((high - low) / close) * 100.0
        if range_pct <= 0.5:
            return True
        if side == "bull":
            if pdh <= 0:
                return False
            gap_pct = ((high - pdh) / pdh) * 100.0
            return gap_pct <= 0.5
        if side == "bear":
            if pdl <= 0:
                return False
            gap_pct = ((pdl - low) / pdl) * 100.0
            return gap_pct <= 0.5
        return False
