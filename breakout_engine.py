
# import logging
# import asyncio
# from datetime import datetime, timedelta
# from math import floor
# import pytz
# from redis_manager import TradeControl

# # --- LOGGING SETUP ---
# logger = logging.getLogger("Nexus_Breakout")
# IST = pytz.timezone("Asia/Kolkata")


# def _safe_create_task(coro, name: str = "task"):
#     """
#     Create a background task and log exceptions (so they don't fail silently).
#     """
#     task = asyncio.create_task(coro)

#     def _done(t: asyncio.Task):
#         try:
#             _ = t.result()
#         except asyncio.CancelledError:
#             return
#         except Exception:
#             logger.exception(f"❌ Background task failed: {name}")

#     task.add_done_callback(_done)
#     return task


# class BreakoutEngine:
#     # Status constants (breakout engine only)
#     ST_WAITING = "WAITING"
#     ST_TRIGGER = "TRIGGER_WATCH"
#     ST_PENDING = "PENDING_ENTRY"
#     ST_OPEN = "OPEN"
#     ST_EXITING = "EXITING"

#     OWNER = "breakout"

#     @staticmethod
#     async def run(token: int, ltp: float, vol: int, state: dict):
#         """
#         Entry point for every tick (called by tick_worker).
#         Handles breakout candle building + signal eval + trade monitoring.
#         """
#         stock = state["stocks"].get(token)
#         if not stock:
#             return

#         # If another engine owns this symbol, breakout should not interfere
#         owner = stock.get("owner")
#         if owner and owner != BreakoutEngine.OWNER:
#             # If breakout is not managing an active breakout position, ignore entirely
#             if stock.get("status") not in (BreakoutEngine.ST_OPEN, BreakoutEngine.ST_PENDING, BreakoutEngine.ST_EXITING):
#                 return

#         # If momentum engine is active on this stock, do not overwrite states/candles
#         if str(stock.get("status", "")).startswith("MOM_"):
#             return

#         # 1) Monitor active breakout trade
#         if stock.get("status") == BreakoutEngine.ST_OPEN:
#             await BreakoutEngine.monitor_active_trade(stock, ltp, state)
#             return

#         # 2) Ignore while placing entry or exiting (prevents duplicate orders)
#         if stock.get("status") in (BreakoutEngine.ST_PENDING, BreakoutEngine.ST_EXITING):
#             return

#         # 3) Trigger watch -> place entry in background (do NOT block tick pipeline)
#         if stock.get("status") == BreakoutEngine.ST_TRIGGER:
#             if stock.get("side_latch") == "BULL" and ltp > float(stock.get("trigger_px", 0) or 0):
#                 logger.info(
#                     f"⚡ [TRIGGER] {stock['symbol']} hit {ltp} (Trigger: {stock.get('trigger_px')})"
#                 )
#                 stock["status"] = BreakoutEngine.ST_PENDING
#                 _safe_create_task(
#                     BreakoutEngine.open_trade(token, stock, ltp, state),
#                     name=f"open_trade:{stock['symbol']}",
#                 )
#             return

#         # 4) Build breakout 1-minute candle (separate keys so momentum can't corrupt)
#         now = datetime.now(IST)
#         bucket = now.replace(second=0, microsecond=0)

#         ckey = "brk_candle"
#         vkey = "brk_last_vol"

#         if stock.get(ckey) and stock[ckey]["bucket"] != bucket:
#             # Candle closed: analyze previous minute in background
#             prev_candle = stock[ckey]
#             _safe_create_task(
#                 BreakoutEngine.analyze_candle_logic(token, prev_candle, state),
#                 name=f"analyze_candle:{stock['symbol']}",
#             )
#             # Start new bucket
#             stock[ckey] = {"bucket": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": 0}
#         elif not stock.get(ckey):
#             stock[ckey] = {"bucket": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": 0}
#         else:
#             c = stock[ckey]
#             c["high"] = max(c["high"], ltp)
#             c["low"] = min(c["low"], ltp)
#             c["close"] = ltp

#             last_vol = int(stock.get(vkey, 0) or 0)
#             if last_vol > 0:
#                 c["volume"] += max(0, int(vol) - last_vol)

#         stock[vkey] = int(vol)

#     @staticmethod
#     async def analyze_candle_logic(token: int, candle: dict, state: dict):
#         """
#         Runs at every minute close (previous bucket candle).
#         """
#         stock = state["stocks"].get(token)
#         if not stock:
#             return

#         # Guard: Bull engine must be ON and PDH must exist
#         if not state["engine_live"].get("bull"):
#             return

#         pdh = float(stock.get("pdh", 0) or 0)
#         if pdh <= 0:
#             return

#         # If some other engine owns this stock, do nothing
#         owner = stock.get("owner")
#         if owner and owner != BreakoutEngine.OWNER:
#             return

#         # Only consider if stock is idle
#         if stock.get("status") not in (BreakoutEngine.ST_WAITING, None, ""):
#             return

#         # LOGIC: candle crossed PDH from below
#         if float(candle.get("open", 0)) < pdh and float(candle.get("close", 0)) > pdh:
#             logger.info(f"🔍 [SCAN] {stock['symbol']} Crossed PDH ({pdh}). Checking Vol Matrix...")

#             is_qualified, detail = await BreakoutEngine.check_vol_matrix(stock, candle, "bull", state)

#             if is_qualified:
#                 logger.info(f"✅ [QUALIFIED] {stock['symbol']} | {detail}")
#                 stock["status"] = BreakoutEngine.ST_TRIGGER
#                 stock["side_latch"] = "BULL"
#                 # stock["trigger_px"] = round(float(candle["high"]) * 1.0005, 2)
#                 stock['trigger_px'] = float(candle['high'])

#                 # ✅ Store reference SL from the qualifying candle (not a future candle)
#                 stock["trigger_sl"] = float(candle["low"])
#                 stock["trigger_bucket"] = candle.get("bucket")
#             else:
#                 logger.info(f"❌ [REJECTED] {stock['symbol']} | {detail}")

#     @staticmethod
#     async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
#         """
#         Validates volume matrix received from Frontend.
#         """
#         matrix = state["config"][side].get("volume_criteria", [])
#         c_vol = float(candle.get("volume", 0) or 0)
#         s_sma = float(stock.get("sma", 0) or 0)
#         close_px = float(candle.get("close", 0) or 0)
#         c_val_cr = (c_vol * close_px) / 10000000.0 if close_px > 0 else 0

#         if not matrix:
#             return True, "Empty Matrix"

#         tier_found = None
#         for i, level in enumerate(matrix):
#             if s_sma >= float(level.get("min_sma_avg", 0) or 0):
#                 tier_found = (i, level)
#             else:
#                 break

#         if tier_found:
#             idx, level = tier_found
#             multiplier = float(level.get("sma_multiplier", 1.0) or 1.0)
#             min_cr = float(level.get("min_vol_price_cr", 0) or 0)
#             required_vol = s_sma * multiplier

#             if c_vol >= required_vol and c_val_cr >= min_cr:
#                 return True, f"T{idx+1} Pass: {c_vol:,.0f} > {required_vol:,.0f}"
#             return False, f"T{idx+1} Fail (Vol or Cr)"

#         return False, f"SMA {s_sma:,.0f} too low"

#     @staticmethod
#     async def open_trade(token: int, stock: dict, ltp: float, state: dict):
#         """
#         Places a real BUY order in Zerodha (market).
#         Runs in background task to avoid blocking tick_worker.
#         """
#         try:
#             # Ownership lock (prevents momentum engine from touching it)
#             stock["owner"] = BreakoutEngine.OWNER

#             side = str(stock.get("side_latch", "BULL")).lower()
#             cfg = state["config"].get(side, {})
#             kite = state.get("kite")

#             if not kite:
#                 logger.error(f"❌ [AUTH ERROR] Kite instance missing for {stock.get('symbol')}")
#                 stock["status"] = BreakoutEngine.ST_WAITING
#                 stock.pop("owner", None)
#                 return

#             # 1) Atomic Trade Limit Check
#             daily_limit = int(cfg.get("total_trades", 5) or 5)
#             if not await TradeControl.can_trade(side, daily_limit):
#                 logger.warning(f"🚫 [LIMIT] {stock['symbol']}: Daily limit reached.")
#                 stock["status"] = BreakoutEngine.ST_WAITING
#                 stock.pop("owner", None)
#                 return

#             # 2) SL from qualifying candle, with hard clamp below entry (prevents immediate exit)
#             raw_sl = float(stock.get("trigger_sl") or ltp)
#             min_gap = max(ltp * 0.0001, 0.01)   # 0.2% or 10p
#             sl_px = min(raw_sl, ltp - min_gap)  # ALWAYS below entry

#             risk_per_share = max(ltp - sl_px, min_gap)

#             total_risk_allowed = float(cfg.get("risk_trade_1", 2000) or 2000)
#             qty = floor(total_risk_allowed / risk_per_share)

#             if qty <= 0:
#                 logger.error(f"⚠️ [SIZE ERROR] {stock['symbol']} Risk too high for risk limit.")
#                 stock["status"] = BreakoutEngine.ST_WAITING
#                 stock.pop("owner", None)
#                 return

#             # 3) Place entry order (blocking API -> thread)
#             order_id = await asyncio.to_thread(
#                 kite.place_order,
#                 variety=kite.VARIETY_REGULAR,
#                 exchange=kite.EXCHANGE_NSE,
#                 tradingsymbol=stock["symbol"],
#                 transaction_type=kite.TRANSACTION_TYPE_BUY,
#                 quantity=qty,
#                 product=kite.PRODUCT_MIS,
#                 order_type=kite.ORDER_TYPE_MARKET,
#             )

#             rr_val = float(str(cfg.get("risk_reward", "1:2")).split(":")[-1])
#             target_price = round(ltp + (risk_per_share * rr_val), 2)

#             now = datetime.now(IST)
#             trade = {
#                 "symbol": stock["symbol"],
#                 "qty": qty,
#                 "entry_price": float(ltp),   # note: best is avg_fill_price from orderbook, but using LTP for now
#                 "sl_price": float(sl_px),
#                 "target_price": float(target_price),
#                 "order_id": order_id,
#                 "pnl": 0.0,
#                 "entry_time": now.strftime("%H:%M:%S"),
#                 # ✅ arming window prevents instant SL/target on same tick
#                 "filled_at": now,
#                 "armed_at": now + timedelta(seconds=0.5),
#                 # ✅ for trailing
#                 "peak": float(ltp),
#             }

#             state["trades"][side].append(trade)
#             stock["active_trade"] = trade
#             stock["status"] = BreakoutEngine.ST_OPEN

#             logger.info(
#                 f"🚀 [REAL ENTRY] {stock['symbol']} Qty: {qty} | OrderID: {order_id} | SL: {sl_px} | TGT: {target_price}"
#             )

#         except Exception as e:
#             logger.exception(f"❌ [ENTRY ERROR] {stock.get('symbol')}: {e}")
#             stock["status"] = BreakoutEngine.ST_WAITING
#             stock["active_trade"] = None
#             stock.pop("owner", None)
#         finally:
#             # Cleanup trigger fields no matter what (avoid stale triggers)
#             stock.pop("trigger_sl", None)
#             stock.pop("trigger_bucket", None)

#     @staticmethod
#     async def monitor_active_trade(stock: dict, ltp: float, state: dict):
#         """
#         Tracks live position for exit signals.
#         """
#         trade = stock.get("active_trade")
#         if not trade:
#             stock["status"] = BreakoutEngine.ST_WAITING
#             stock.pop("owner", None)
#             return

#         # If we're already exiting, do nothing
#         if stock.get("status") == BreakoutEngine.ST_EXITING:
#             return

#         # Update PnL + peak
#         entry = float(trade.get("entry_price", 0) or 0)
#         qty = int(trade.get("qty", 0) or 0)
#         trade["pnl"] = round((ltp - entry) * qty, 2)
#         trade["peak"] = max(float(trade.get("peak", entry) or entry), float(ltp))

#         # Manual dashboard exit should be immediate
#         if stock.get("symbol") in state.get("manual_exits", set()):
#             state["manual_exits"].discard(stock["symbol"])
#             logger.info(f"🖱️ [MANUAL EXIT] {stock['symbol']}")
#             _safe_create_task(
#                 BreakoutEngine.close_position(stock, state, "MANUAL"),
#                 name=f"close_position:MANUAL:{stock['symbol']}",
#             )
#             return

#         # Arming window (prevents immediate SL/target right after entry)
#         now = datetime.now(IST)
#         armed_at = trade.get("armed_at")
#         if armed_at and isinstance(armed_at, datetime) and now < armed_at:
#             return

#         # Exit checks
#         target = float(trade.get("target_price", 0) or 0)
#         sl = float(trade.get("sl_price", 0) or 0)

#         if ltp >= target > 0:
#             logger.info(f"🎯 [TARGET] {stock['symbol']} reached {target} (LTP {ltp})")
#             _safe_create_task(
#                 BreakoutEngine.close_position(stock, state, "TARGET"),
#                 name=f"close_position:TARGET:{stock['symbol']}",
#             )
#             return

#         if ltp <= sl:
#             logger.info(f"🛑 [STOPLOSS] {stock['symbol']} hit SL {sl} (LTP {ltp})")
#             _safe_create_task(
#                 BreakoutEngine.close_position(stock, state, "SL"),
#                 name=f"close_position:SL:{stock['symbol']}",
#             )
#             return

#         # Trailing SL (peak-based, never cross current price)
#         side = str(stock.get("side_latch", "BULL")).lower()
#         cfg = state["config"].get(side, {})
#         tsl_ratio = float(str(cfg.get("trailing_sl", "1:1.5")).split(":")[-1])

#         new_sl = await BreakoutEngine.calculate_tsl(trade, tsl_ratio)

#         # keep SL below current price by buffer
#         buffer = max(ltp * 0.001, 0.10)
#         new_sl = min(float(new_sl), float(ltp) - buffer)

#         if new_sl > float(trade["sl_price"]):
#             trade["sl_price"] = round(new_sl, 2)

#     @staticmethod
#     async def calculate_tsl(trade: dict, ratio: float):
#         """
#         Peak-based TSL:
#         - starts trailing only after profit > risk * ratio
#         - trails at (peak - risk*0.8)
#         """
#         entry = float(trade.get("entry_price", 0) or 0)
#         sl = float(trade.get("sl_price", 0) or 0)
#         peak = float(trade.get("peak", entry) or entry)

#         # Ensure risk positive (fallback to 0.2% if needed)
#         risk = max(entry - sl, entry * 0.002)
#         profit = peak - entry

#         if profit > (risk * ratio):
#             return peak - (risk * 0.8)

#         return sl

#     @staticmethod
#     async def close_position(stock: dict, state: dict, reason: str):
#         """
#         Places a real SELL order to exit the position.
#         Runs in background task to avoid blocking tick_worker.
#         """
#         # Deduplicate exits
#         if stock.get("status") == BreakoutEngine.ST_EXITING:
#             return
#         stock["status"] = BreakoutEngine.ST_EXITING

#         trade = stock.get("active_trade")
#         kite = state.get("kite")

#         if trade and kite:
#             try:
#                 exit_id = await asyncio.to_thread(
#                     kite.place_order,
#                     variety=kite.VARIETY_REGULAR,
#                     exchange=kite.EXCHANGE_NSE,
#                     tradingsymbol=stock["symbol"],
#                     transaction_type=kite.TRANSACTION_TYPE_SELL,
#                     quantity=int(trade["qty"]),
#                     product=kite.PRODUCT_MIS,
#                     order_type=kite.ORDER_TYPE_MARKET,
#                 )
#                 logger.info(f"🏁 [REAL EXIT] {stock['symbol']} Reason: {reason} | OrderID: {exit_id}")
#             except Exception as e:
#                 logger.exception(f"❌ [KITE EXIT ERROR] {stock.get('symbol')}: {e}")

#         # Cleanup RAM state (release ownership)
#         stock["status"] = BreakoutEngine.ST_WAITING
#         stock["active_trade"] = None
#         stock.pop("owner", None)
#         stock.pop("trigger_px", None)
#         stock.pop("side_latch", None)
#         stock.pop("trigger_sl", None)
#         stock.pop("trigger_bucket", None)

import logging
import asyncio
from datetime import datetime, timedelta
from math import floor
import pytz
from redis_manager import TradeControl

# --- LOGGING SETUP ---
logger = logging.getLogger("Nexus_Breakout")
IST = pytz.timezone("Asia/Kolkata")


def _safe_create_task(coro, name: str = "task"):
    """Create a background task and log exceptions (so they don't fail silently)."""
    task = asyncio.create_task(coro)

    def _done(t: asyncio.Task):
        try:
            _ = t.result()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception(f"❌ Background task failed: {name}")

    task.add_done_callback(_done)
    return task


class BreakoutEngine:
    # Status constants
    ST_WAITING = "WAITING"
    ST_TRIGGER = "TRIGGER_WATCH"
    ST_PENDING = "PENDING_ENTRY"
    ST_OPEN = "OPEN"
    ST_EXITING = "EXITING"

    OWNER = "breakout"

    # ----------------------------
    # TUNABLE BUFFERS
    # ----------------------------
    # Entry trigger buffer (set 0.0 to remove entry buffer completely)
    ENTRY_BUFFER_PCT = 0.0005   # 0.05%

    # Exit buffers (you asked for 0.01% buffer)
    EXIT_MIN_GAP_PCT = 0.0001   # 0.01%
    EXIT_MIN_GAP_ABS = 0.01     # ₹0.01 minimum

    TRAIL_CLAMP_PCT = 0.0001    # 0.01%
    TRAIL_CLAMP_ABS = 0.01      # ₹0.01 minimum

    # How tight SL trails once activated (0.8 => SL trails at 80% of initial risk)
    TRAIL_RISK_FACTOR = 0.8

    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        """Called on every tick (from tick_worker)."""
        stock = state["stocks"].get(token)
        if not stock:
            return

        # If another engine owns this symbol, breakout must not interfere
        owner = stock.get("owner")
        if owner and owner != BreakoutEngine.OWNER:
            if stock.get("status") not in (BreakoutEngine.ST_OPEN, BreakoutEngine.ST_PENDING, BreakoutEngine.ST_EXITING):
                return

        # If momentum engine states are active, do not interfere
        if str(stock.get("status", "")).startswith("MOM_"):
            return

        # 1) Monitor open trade
        if stock.get("status") == BreakoutEngine.ST_OPEN:
            await BreakoutEngine.monitor_active_trade(stock, ltp, state)
            return

        # 2) Ignore while entry placing or exiting
        if stock.get("status") in (BreakoutEngine.ST_PENDING, BreakoutEngine.ST_EXITING):
            return

        # 3) Trigger watch -> background entry
        if stock.get("status") == BreakoutEngine.ST_TRIGGER:
            side_latch = stock.get("side_latch")
            trigger_px = float(stock.get("trigger_px", 0) or 0)

            if side_latch == "BULL" and ltp >= trigger_px:
                logger.info(f"⚡ [TRIGGER-BULL] {stock['symbol']} hit {ltp} (Trigger: {trigger_px})")
                stock["status"] = BreakoutEngine.ST_PENDING
                _safe_create_task(
                    BreakoutEngine.open_trade(token, stock, ltp, state),
                    name=f"open_trade:BULL:{stock['symbol']}",
                )

            elif side_latch == "BEAR" and ltp <= trigger_px:
                logger.info(f"⚡ [TRIGGER-BEAR] {stock['symbol']} hit {ltp} (Trigger: {trigger_px})")
                stock["status"] = BreakoutEngine.ST_PENDING
                _safe_create_task(
                    BreakoutEngine.open_trade(token, stock, ltp, state),
                    name=f"open_trade:BEAR:{stock['symbol']}",
                )

            return

        # 4) Build breakout 1-minute candle (separate keys so momentum can't corrupt)
        now = datetime.now(IST)
        bucket = now.replace(second=0, microsecond=0)

        ckey = "brk_candle"
        vkey = "brk_last_vol"

        if stock.get(ckey) and stock[ckey]["bucket"] != bucket:
            prev_candle = stock[ckey]
            _safe_create_task(
                BreakoutEngine.analyze_candle_logic(token, prev_candle, state),
                name=f"analyze_breakout:{stock['symbol']}",
            )
            stock[ckey] = {"bucket": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": 0}
        elif not stock.get(ckey):
            stock[ckey] = {"bucket": bucket, "open": ltp, "high": ltp, "low": ltp, "close": ltp, "volume": 0}
        else:
            c = stock[ckey]
            c["high"] = max(c["high"], ltp)
            c["low"] = min(c["low"], ltp)
            c["close"] = ltp

            last_vol = int(stock.get(vkey, 0) or 0)
            if last_vol > 0:
                c["volume"] += max(0, int(vol) - last_vol)

        stock[vkey] = int(vol)

    @staticmethod
    async def analyze_candle_logic(token: int, candle: dict, state: dict):
        """Runs at every minute close (previous bucket candle). Adds BOTH bull & bear breakout."""
        stock = state["stocks"].get(token)
        if not stock:
            return

        # Only scan when idle (avoid overwriting existing states)
        if stock.get("status") not in (BreakoutEngine.ST_WAITING, None, ""):
            return

        # If another engine owns this symbol, skip
        owner = stock.get("owner")
        if owner and owner != BreakoutEngine.OWNER:
            return

        o = float(candle.get("open", 0) or 0)
        c = float(candle.get("close", 0) or 0)
        h = float(candle.get("high", 0) or 0)
        l = float(candle.get("low", 0) or 0)
        if o <= 0:
            return

        # ----------------------------
        # BULL BREAKOUT (PDH cross)
        # ----------------------------
        if state["engine_live"].get("bull"):
            pdh = float(stock.get("pdh", 0) or 0)
            if pdh > 0:
                # Candle crossed PDH from below
                if o < pdh and c > pdh:
                    logger.info(f"🔍 [SCAN-BULL] {stock['symbol']} Crossed PDH ({pdh}). Checking Vol Matrix...")
                    is_qualified, detail = await BreakoutEngine.check_vol_matrix(stock, candle, "bull", state)

                    if is_qualified:
                        # Trigger above candle high (buffer optional)
                        trigger = h * (1.0 + BreakoutEngine.ENTRY_BUFFER_PCT)
                        stock["status"] = BreakoutEngine.ST_TRIGGER
                        stock["side_latch"] = "BULL"
                        stock["trigger_px"] = round(trigger, 2)

                        # Reference SL from qualifying candle (low)
                        stock["trigger_sl"] = float(l)
                        stock["trigger_bucket"] = candle.get("bucket")

                        logger.info(f"✅ [QUALIFIED-BULL] {stock['symbol']} | {detail} | TRG:{stock['trigger_px']}")
                    else:
                        logger.info(f"❌ [REJECT-BULL] {stock['symbol']} | {detail}")

        # ----------------------------
        # BEAR BREAKOUT (PDL cross)
        # ----------------------------
        if state["engine_live"].get("bear"):
            pdl = float(stock.get("pdl", 0) or 0)
            if pdl > 0:
                # Candle crossed PDL from above
                if o > pdl and c < pdl:
                    logger.info(f"🔍 [SCAN-BEAR] {stock['symbol']} Crossed PDL ({pdl}). Checking Vol Matrix...")
                    is_qualified, detail = await BreakoutEngine.check_vol_matrix(stock, candle, "bear", state)

                    if is_qualified:
                        # Trigger below candle low (buffer optional)
                        trigger = l * (1.0 - BreakoutEngine.ENTRY_BUFFER_PCT)
                        stock["status"] = BreakoutEngine.ST_TRIGGER
                        stock["side_latch"] = "BEAR"
                        stock["trigger_px"] = round(trigger, 2)

                        # Reference SL from qualifying candle (high)
                        stock["trigger_sl"] = float(h)
                        stock["trigger_bucket"] = candle.get("bucket")

                        logger.info(f"✅ [QUALIFIED-BEAR] {stock['symbol']} | {detail} | TRG:{stock['trigger_px']}")
                    else:
                        logger.info(f"❌ [REJECT-BEAR] {stock['symbol']} | {detail}")

    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
        """Validates volume matrix received from Frontend."""
        matrix = state["config"][side].get("volume_criteria", [])
        c_vol = float(candle.get("volume", 0) or 0)
        s_sma = float(stock.get("sma", 0) or 0)
        close_px = float(candle.get("close", 0) or 0)
        c_val_cr = (c_vol * close_px) / 10000000.0 if close_px > 0 else 0

        if not matrix:
            return True, "Empty Matrix"

        tier_found = None
        for i, level in enumerate(matrix):
            if s_sma >= float(level.get("min_sma_avg", 0) or 0):
                tier_found = (i, level)
            else:
                break

        if tier_found:
            idx, level = tier_found
            multiplier = float(level.get("sma_multiplier", 1.0) or 1.0)
            min_cr = float(level.get("min_vol_price_cr", 0) or 0)
            required_vol = s_sma * multiplier

            if c_vol >= required_vol and c_val_cr >= min_cr:
                return True, f"T{idx+1} Pass: {c_vol:,.0f} > {required_vol:,.0f}"
            return False, f"T{idx+1} Fail (Vol or Cr)"

        return False, f"SMA {s_sma:,.0f} too low"

    @staticmethod
    async def open_trade(token: int, stock: dict, ltp: float, state: dict):
        """Places a real BUY (bull) or SELL (bear) order in Zerodha (market)."""
        try:
            stock["owner"] = BreakoutEngine.OWNER  # lock ownership

            side_latch = stock.get("side_latch", "BULL")
            side_key = "bull" if side_latch == "BULL" else "bear"
            is_bull = (side_key == "bull")

            cfg = state["config"].get(side_key, {})
            kite = state.get("kite")

            if not kite:
                logger.error(f"❌ [AUTH ERROR] Kite missing for {stock.get('symbol')}")
                stock["status"] = BreakoutEngine.ST_WAITING
                stock.pop("owner", None)
                return

            # Trade limit
            daily_limit = int(cfg.get("total_trades", 5) or 5)
            if not await TradeControl.can_trade(side_key, daily_limit):
                logger.warning(f"🚫 [LIMIT] {stock['symbol']}: Daily limit reached ({side_key}).")
                stock["status"] = BreakoutEngine.ST_WAITING
                stock.pop("owner", None)
                return

            # SL reference from qualifying candle
            raw_sl = float(stock.get("trigger_sl") or ltp)

            # Exit min gap buffer (0.01% requested)
            min_gap = max(ltp * BreakoutEngine.EXIT_MIN_GAP_PCT, BreakoutEngine.EXIT_MIN_GAP_ABS)

            if is_bull:
                # SL must be below entry
                sl_px = min(raw_sl, ltp - min_gap)
                risk_per_share = max(ltp - sl_px, min_gap)
            else:
                # SL must be above entry (short)
                sl_px = max(raw_sl, ltp + min_gap)
                risk_per_share = max(sl_px - ltp, min_gap)

            total_risk_allowed = float(cfg.get("risk_trade_1", 2000) or 2000)
            qty = floor(total_risk_allowed / risk_per_share)

            if qty <= 0:
                logger.error(f"⚠️ [SIZE ERROR] {stock['symbol']} Risk too high for risk limit.")
                stock["status"] = BreakoutEngine.ST_WAITING
                stock.pop("owner", None)
                return

            # Place order
            order_id = await asyncio.to_thread(
                kite.place_order,
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=stock["symbol"],
                transaction_type=kite.TRANSACTION_TYPE_BUY if is_bull else kite.TRANSACTION_TYPE_SELL,
                quantity=qty,
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET,
            )

            rr_val = float(str(cfg.get("risk_reward", "1:2")).split(":")[-1])
            if is_bull:
                target_price = round(ltp + (risk_per_share * rr_val), 2)
            else:
                target_price = round(ltp - (risk_per_share * rr_val), 2)

            now = datetime.now(IST)
            trade = {
                "symbol": stock["symbol"],
                "qty": qty,
                "entry_price": float(ltp),
                "sl_price": float(sl_px),
                "target_price": float(target_price),
                "order_id": order_id,
                "pnl": 0.0,
                "entry_time": now.strftime("%H:%M:%S"),
                "filled_at": now,
                "armed_at": now + timedelta(seconds=0.5),  # protect from instant exit
                "side_key": side_key,
                # For trailing
                "peak": float(ltp),     # bull
                "trough": float(ltp),   # bear
            }

            state["trades"][side_key].append(trade)
            stock["active_trade"] = trade
            stock["status"] = BreakoutEngine.ST_OPEN

            logger.info(
                f"🚀 [ENTRY-{side_key.upper()}] {stock['symbol']} Qty:{qty} "
                f"| OID:{order_id} | Entry:{ltp} | SL:{sl_px} | TGT:{target_price}"
            )

        except Exception as e:
            logger.exception(f"❌ [ENTRY ERROR] {stock.get('symbol')}: {e}")
            stock["status"] = BreakoutEngine.ST_WAITING
            stock["active_trade"] = None
            stock.pop("owner", None)
        finally:
            # Clear trigger fields to avoid stale signals
            stock.pop("trigger_sl", None)
            stock.pop("trigger_bucket", None)

    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        """Tracks live position for exit signals (supports bull + bear)."""
        trade = stock.get("active_trade")
        if not trade:
            stock["status"] = BreakoutEngine.ST_WAITING
            stock.pop("owner", None)
            return

        if stock.get("status") == BreakoutEngine.ST_EXITING:
            return

        side_key = str(trade.get("side_key") or "bull").lower()
        is_bull = (side_key == "bull")
        cfg = state["config"].get(side_key, {})

        entry = float(trade.get("entry_price", 0) or 0)
        qty = int(trade.get("qty", 0) or 0)

        # Manual exit first
        if stock.get("symbol") in state.get("manual_exits", set()):
            state["manual_exits"].discard(stock["symbol"])
            logger.info(f"🖱️ [MANUAL EXIT] {stock['symbol']}")
            _safe_create_task(
                BreakoutEngine.close_position(stock, state, "MANUAL"),
                name=f"close_position:MANUAL:{stock['symbol']}",
            )
            return

        # Arming window skip
        now = datetime.now(IST)
        armed_at = trade.get("armed_at")
        if armed_at and isinstance(armed_at, datetime) and now < armed_at:
            # update extrema + pnl even during arming (optional)
            if is_bull:
                trade["peak"] = max(float(trade.get("peak", entry) or entry), float(ltp))
                trade["pnl"] = round((ltp - entry) * qty, 2)
            else:
                trade["trough"] = min(float(trade.get("trough", entry) or entry), float(ltp))
                trade["pnl"] = round((entry - ltp) * qty, 2)
            return

        # Update extrema
        if is_bull:
            trade["peak"] = max(float(trade.get("peak", entry) or entry), float(ltp))
        else:
            trade["trough"] = min(float(trade.get("trough", entry) or entry), float(ltp))

        # Update PnL
        if is_bull:
            trade["pnl"] = round((ltp - entry) * qty, 2)
        else:
            trade["pnl"] = round((entry - ltp) * qty, 2)

        target = float(trade.get("target_price", 0) or 0)
        sl = float(trade.get("sl_price", 0) or 0)

        # Exit checks
        if is_bull:
            if target > 0 and ltp >= target:
                logger.info(f"🎯 [TARGET-BULL] {stock['symbol']} TGT:{target} LTP:{ltp}")
                _safe_create_task(
                    BreakoutEngine.close_position(stock, state, "TARGET"),
                    name=f"close_position:TARGET:{stock['symbol']}",
                )
                return

            if ltp <= sl:
                logger.info(f"🛑 [SL-BULL] {stock['symbol']} SL:{sl} LTP:{ltp}")
                _safe_create_task(
                    BreakoutEngine.close_position(stock, state, "SL"),
                    name=f"close_position:SL:{stock['symbol']}",
                )
                return
        else:
            if target > 0 and ltp <= target:
                logger.info(f"🎯 [TARGET-BEAR] {stock['symbol']} TGT:{target} LTP:{ltp}")
                _safe_create_task(
                    BreakoutEngine.close_position(stock, state, "TARGET"),
                    name=f"close_position:TARGET:{stock['symbol']}",
                )
                return

            if ltp >= sl:
                logger.info(f"🛑 [SL-BEAR] {stock['symbol']} SL:{sl} LTP:{ltp}")
                _safe_create_task(
                    BreakoutEngine.close_position(stock, state, "SL"),
                    name=f"close_position:SL:{stock['symbol']}",
                )
                return

        # Trailing SL
        tsl_ratio = float(str(cfg.get("trailing_sl", "1:1.5")).split(":")[-1])
        new_sl = await BreakoutEngine.calculate_tsl(trade, tsl_ratio, is_bull)

        # Clamp SL away from current price (0.01% requested)
        clamp_buf = max(ltp * BreakoutEngine.TRAIL_CLAMP_PCT, BreakoutEngine.TRAIL_CLAMP_ABS)

        if is_bull:
            # SL must stay below price
            new_sl = min(float(new_sl), float(ltp) - clamp_buf)
            if new_sl > float(trade["sl_price"]):
                trade["sl_price"] = round(new_sl, 2)
        else:
            # SL must stay above price for shorts
            new_sl = max(float(new_sl), float(ltp) + clamp_buf)
            if new_sl < float(trade["sl_price"]):
                trade["sl_price"] = round(new_sl, 2)

    @staticmethod
    async def calculate_tsl(trade: dict, ratio: float, is_bull: bool):
        """
        Peak/trough-based trailing:
        - starts trailing only after profit > risk * ratio
        - then trails at peak - risk*factor (bull) or trough + risk*factor (bear)
        """
        entry = float(trade.get("entry_price", 0) or 0)
        sl = float(trade.get("sl_price", 0) or 0)

        if is_bull:
            peak = float(trade.get("peak", entry) or entry)
            risk = max(entry - sl, entry * BreakoutEngine.EXIT_MIN_GAP_PCT)
            profit = peak - entry

            if profit > (risk * ratio):
                return peak - (risk * BreakoutEngine.TRAIL_RISK_FACTOR)
            return sl

        trough = float(trade.get("trough", entry) or entry)
        risk = max(sl - entry, entry * BreakoutEngine.EXIT_MIN_GAP_PCT)
        profit = entry - trough

        if profit > (risk * ratio):
            return trough + (risk * BreakoutEngine.TRAIL_RISK_FACTOR)
        return sl

    @staticmethod
    async def close_position(stock: dict, state: dict, reason: str):
        """Places a real market order to exit (SELL for bull, BUY for bear)."""
        if stock.get("status") == BreakoutEngine.ST_EXITING:
            return
        stock["status"] = BreakoutEngine.ST_EXITING

        trade = stock.get("active_trade")
        kite = state.get("kite")

        side_key = str(trade.get("side_key") if trade else "bull").lower()
        is_bull = (side_key == "bull")

        if trade and kite:
            try:
                exit_id = await asyncio.to_thread(
                    kite.place_order,
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=stock["symbol"],
                    transaction_type=kite.TRANSACTION_TYPE_SELL if is_bull else kite.TRANSACTION_TYPE_BUY,
                    quantity=int(trade["qty"]),
                    product=kite.PRODUCT_MIS,
                    order_type=kite.ORDER_TYPE_MARKET,
                )
                logger.info(f"🏁 [EXIT-{side_key.upper()}] {stock['symbol']} Reason:{reason} | OID:{exit_id}")
            except Exception as e:
                logger.exception(f"❌ [KITE EXIT ERROR] {stock.get('symbol')}: {e}")

        # Cleanup
        stock["status"] = BreakoutEngine.ST_WAITING
        stock["active_trade"] = None
        stock.pop("owner", None)
        stock.pop("trigger_px", None)
        stock.pop("side_latch", None)
        stock.pop("trigger_sl", None)
        stock.pop("trigger_bucket", None)
