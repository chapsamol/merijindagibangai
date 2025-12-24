import asyncio
import logging
from datetime import datetime
from math import floor
from typing import Optional
import pytz

from redis_manager import TradeControl

logger = logging.getLogger("Nexus_Breakout")
IST = pytz.timezone("Asia/Kolkata")


class BreakoutEngine:
    """
    Breakout Engine (Bull/Bear)

    Fixes implemented:
      ✅ Uses per-engine keys (brk_*) to avoid collision with MomentumEngine
      ✅ Trigger watch TTL = 6 minutes (per symbol)
      ✅ Per-symbol trades/day cap = 2 (Redis, survives restart)
      ✅ 2nd trade only after 1st closed (Redis open-lock)
      ✅ Side limit total_trades is atomic & rollback-safe
      ✅ Trades are marked CLOSED (not removed) so PnL stays correct
      ✅ Candle logic is called from main tick_worker on candle-close
    """

    EXIT_BUFFER_PCT = 0.0001
    TRIGGER_VALID_SECONDS = 6 * 60
    MAX_TRADES_PER_SYMBOL = 2

    # -----------------------------
    # TICK HANDLER (no candle aggregation here)
    # -----------------------------
    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        stock = state["stocks"].get(token)
        if not stock:
            return

        stock["ltp"] = float(ltp or 0.0)

        brk_status = stock.get("brk_status", "WAITING")

        # monitor open trade regardless of engine toggle
        if brk_status == "OPEN":
            await BreakoutEngine.monitor_active_trade(stock, ltp, state)
            return

        # trigger watch
        if brk_status == "TRIGGER_WATCH":
            side = (stock.get("brk_side_latch") or "").lower()
            if side not in ("bull", "bear"):
                BreakoutEngine._reset_waiting(stock)
                return

            # TTL
            now_ts = int(datetime.now(IST).timestamp())
            set_ts = int(stock.get("brk_trigger_set_ts") or 0)
            if set_ts and (now_ts - set_ts) > BreakoutEngine.TRIGGER_VALID_SECONDS:
                logger.info(f"⏳ [BRK-EXPIRE] {stock.get('symbol')} {side.upper()} trigger expired (>6m).")
                BreakoutEngine._reset_waiting(stock)
                return

            # engine toggle gates new entry only
            if not bool(state["engine_live"].get(side, True)):
                return

            # trade window gate
            if not BreakoutEngine._within_trade_window(state["config"].get(side, {})):
                BreakoutEngine._reset_waiting(stock)
                return

            trig = float(stock.get("brk_trigger_px", 0.0) or 0.0)
            if trig <= 0:
                BreakoutEngine._reset_waiting(stock)
                return

            if side == "bull" and ltp > trig:
                await BreakoutEngine.open_trade(stock, ltp, state, "bull")
            elif side == "bear" and ltp < trig:
                await BreakoutEngine.open_trade(stock, ltp, state, "bear")
            return

    # -----------------------------
    # CANDLE-CLOSE QUALIFICATION (called by main.py)
    # -----------------------------
    @staticmethod
    async def on_candle_close(token: int, candle: dict, state: dict):
        stock = state["stocks"].get(token)
        if not stock:
            return

        symbol = stock.get("symbol")
        if not symbol:
            return

        # Do not qualify if breakout already has open/trigger watch
        if stock.get("brk_status") in ("OPEN", "TRIGGER_WATCH"):
            return

        # If any engine has open lock for symbol in Redis, you still *can* qualify,
        # but entry will be blocked. Keeping it simple: qualify allowed.
        # (If you want: skip qualification when a position is open elsewhere.)

        # Per-symbol cap check (Redis)
        taken = await TradeControl.get_symbol_trade_count(symbol)
        if taken >= BreakoutEngine.MAX_TRADES_PER_SYMBOL:
            return

        pdh = float(stock.get("pdh", 0) or 0)
        pdl = float(stock.get("pdl", 0) or 0)
        if pdh <= 0 or pdl <= 0:
            return

        high = float(candle.get("high", 0) or 0)
        low = float(candle.get("low", 0) or 0)
        close = float(candle.get("close", 0) or 0)
        c_vol = int(candle.get("volume", 0) or 0)
        if close <= 0 or high <= 0 or low <= 0:
            return

        now = datetime.now(IST)

        # BULL
        if close > pdh:
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

            stock["brk_status"] = "TRIGGER_WATCH"
            stock["brk_side_latch"] = "bull"
            stock["brk_trigger_px"] = float(high)
            stock["brk_trigger_set_ts"] = int(now.timestamp())
            stock["brk_trigger_candle"] = dict(candle)

            stock["brk_scan_vol"] = int(c_vol)
            stock["brk_scan_reason"] = f"PDH break + Vol OK ({detail})"
            return

        # BEAR
        if close < pdl:
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

            stock["brk_status"] = "TRIGGER_WATCH"
            stock["brk_side_latch"] = "bear"
            stock["brk_trigger_px"] = float(low)
            stock["brk_trigger_set_ts"] = int(now.timestamp())
            stock["brk_trigger_candle"] = dict(candle)

            stock["brk_scan_vol"] = int(c_vol)
            stock["brk_scan_reason"] = f"PDL break + Vol OK ({detail})"
            return

    # -----------------------------
    # VOLUME MATRIX
    # -----------------------------
    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
        cfg = state["config"].get(side, {})
        matrix = cfg.get("volume_criteria", []) or []

        c_vol = int(candle.get("volume", 0) or 0)
        s_sma = float(stock.get("sma", 0) or 0)
        close = float(candle.get("close", 0) or 0)
        c_val_cr = (c_vol * close) / 10000000.0 if close > 0 else 0.0

        if not matrix:
            return True, "NoMatrix"

        tier_found = None
        for i, level in enumerate(matrix):
            min_sma_avg = float(level.get("min_sma_avg", 0) or 0)
            if s_sma >= min_sma_avg:
                tier_found = (i, level)
            else:
                break

        if not tier_found:
            return False, f"SMA {s_sma:,.0f} too low"

        idx, level = tier_found
        required_vol = s_sma * float(level.get("sma_multiplier", 1.0) or 1.0)
        min_cr = float(level.get("min_vol_price_cr", 0) or 0)

        if c_vol >= required_vol and c_val_cr >= min_cr:
            return True, f"Tier{idx+1}Pass"
        return False, f"Tier{idx+1}Fail"

    # -----------------------------
    # OPEN TRADE
    # -----------------------------
    @staticmethod
    async def open_trade(stock: dict, ltp: float, state: dict, side_key: str):
        symbol = stock.get("symbol") or ""
        if not symbol:
            BreakoutEngine._reset_waiting(stock)
            return

        cfg = state["config"].get(side_key, {})
        kite = state.get("kite")
        if not kite:
            BreakoutEngine._reset_waiting(stock)
            return

        if not bool(state["engine_live"].get(side_key, True)):
            BreakoutEngine._reset_waiting(stock)
            return
        if not BreakoutEngine._within_trade_window(cfg):
            BreakoutEngine._reset_waiting(stock)
            return

        # 1) side limit reservation (atomic)
        side_limit = int(cfg.get("total_trades", 5) or 5)
        if not await TradeControl.reserve_side_trade(side_key, side_limit):
            BreakoutEngine._reset_waiting(stock)
            return

        # 2) per-symbol reservation (atomic + open lock)
        ok, reason = await TradeControl.reserve_symbol_trade(symbol, max_trades=BreakoutEngine.MAX_TRADES_PER_SYMBOL)
        if not ok:
            await TradeControl.rollback_side_trade(side_key)
            BreakoutEngine._reset_waiting(stock)
            return

        is_bull = (side_key == "bull")

        trig_candle = stock.get("brk_trigger_candle") or {}
        sl_px = float(trig_candle.get("low", 0) or 0) if is_bull else float(trig_candle.get("high", 0) or 0)

        entry = float(ltp)
        if sl_px <= 0:
            sl_px = round(entry * (0.995 if is_bull else 1.005), 2)

        risk_per_share = max(abs(entry - sl_px), entry * 0.005)
        risk_amount = float(cfg.get("risk_trade_1", 2000) or 2000)
        qty = floor(risk_amount / risk_per_share)
        if qty <= 0:
            await TradeControl.rollback_symbol_trade(symbol)
            await TradeControl.rollback_side_trade(side_key)
            BreakoutEngine._reset_waiting(stock)
            return

        try:
            order_id = await asyncio.to_thread(
                kite.place_order,
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol=symbol,
                transaction_type=(kite.TRANSACTION_TYPE_BUY if is_bull else kite.TRANSACTION_TYPE_SELL),
                quantity=qty,
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET,
            )

            rr_val = float(str(cfg.get("risk_reward", "1:2")).split(":")[-1])
            target = round(entry + (risk_per_share * rr_val), 2) if is_bull else round(entry - (risk_per_share * rr_val), 2)

            tsl_ratio = float(str(cfg.get("trailing_sl", "1:1.5")).split(":")[-1])
            trail_step = float(risk_per_share * tsl_ratio) if tsl_ratio > 0 else float(risk_per_share)

            trade = {
                "engine": "breakout",
                "side": side_key,
                "symbol": symbol,
                "qty": int(qty),
                "entry_price": float(entry),
                "sl_price": float(sl_px),
                "target_price": float(target),
                "order_id": order_id,
                "pnl": 0.0,
                "status": "OPEN",
                "entry_time": datetime.now(IST).strftime("%H:%M:%S"),
                "init_risk": float(risk_per_share),
                "trail_step": float(trail_step),
            }

            state["trades"][side_key].append(trade)

            stock["brk_status"] = "OPEN"
            stock["brk_active_trade"] = trade
            stock["brk_side_latch"] = side_key

            # clear scanner
            stock["brk_scan_seen_ts"] = None
            stock["brk_scan_seen_time"] = None

        except Exception as e:
            logger.error(f"❌ [BRK ORDER ERROR] {symbol}: {e}")
            await TradeControl.rollback_symbol_trade(symbol)
            await TradeControl.rollback_side_trade(side_key)
            BreakoutEngine._reset_waiting(stock)

    # -----------------------------
    # MONITOR / EXIT
    # -----------------------------
    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        trade = stock.get("brk_active_trade")
        if not trade:
            return

        side_key = (stock.get("brk_side_latch") or "").lower()
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
            await BreakoutEngine.close_position(stock, state, "TARGET")
            return

        if sl_hit:
            await BreakoutEngine.close_position(stock, state, "SL")
            return

        # trail
        new_sl = BreakoutEngine._step_trail_sl(trade, float(ltp), is_bull)
        if new_sl is not None:
            cur = float(trade.get("sl_price", 0) or 0)
            if is_bull and new_sl > cur:
                trade["sl_price"] = float(new_sl)
            elif (not is_bull) and new_sl < cur:
                trade["sl_price"] = float(new_sl)

        if stock.get("symbol") in state.get("manual_exits", set()):
            await BreakoutEngine.close_position(stock, state, "MANUAL")
            state["manual_exits"].remove(stock["symbol"])

    @staticmethod
    def _step_trail_sl(trade: dict, ltp: float, is_bull: bool):
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
        kite = state.get("kite")
        symbol = stock.get("symbol") or ""
        side_key = (stock.get("brk_side_latch") or "").lower()
        is_bull = (side_key == "bull")

        if trade and kite and symbol:
            try:
                exit_id = await asyncio.to_thread(
                    kite.place_order,
                    variety=kite.VARIETY_REGULAR,
                    exchange=kite.EXCHANGE_NSE,
                    tradingsymbol=symbol,
                    transaction_type=(kite.TRANSACTION_TYPE_SELL if is_bull else kite.TRANSACTION_TYPE_BUY),
                    quantity=int(trade["qty"]),
                    product=kite.PRODUCT_MIS,
                    order_type=kite.ORDER_TYPE_MARKET,
                )
                trade["exit_order_id"] = exit_id
            except Exception as e:
                logger.error(f"❌ [BRK EXIT ERROR] {symbol}: {e}")

        if trade:
            trade["status"] = "CLOSED"
            trade["exit_time"] = datetime.now(IST).strftime("%H:%M:%S")
            trade["exit_reason"] = reason

        # release per-symbol open lock so 2nd trade can occur
        if symbol:
            await TradeControl.release_symbol_lock(symbol)

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

    @staticmethod
    def _within_trade_window(cfg: dict, now: Optional[datetime] = None) -> bool:
        from datetime import time as dtime
        try:
            now = now or datetime.now(IST)
            start_s = str(cfg.get("trade_start", "09:15"))
            end_s = str(cfg.get("trade_end", "15:10"))
            sh, sm = map(int, start_s.split(":"))
            eh, em = map(int, end_s.split(":"))
            start_t = dtime(sh, sm)
            end_t = dtime(eh, em)
            nt = now.time()
            return (nt >= start_t) and (nt <= end_t)
        except Exception:
            return True

    @staticmethod
    def _range_gate_ok(side: str, high: float, low: float, close: float, *, pdh: float, pdl: float) -> bool:
        if close <= 0:
            return False
        range_pct = ((high - low) / close) * 100.0
        if range_pct <= 0.7:
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
