import asyncio
import logging
from datetime import datetime
from math import floor
from typing import Optional
import pytz

from redis_manager import TradeControl

logger = logging.getLogger("Nexus_Momentum")
IST = pytz.timezone("Asia/Kolkata")


class MomentumEngine:
    """
    Momentum Engine (Bull/Bear)

    Fixes implemented:
      ✅ Uses per-engine keys (mom_*) to avoid collision with BreakoutEngine
      ✅ Per-symbol trades/day cap = 2 (Redis)
      ✅ 2nd trade only after 1st closed (Redis open-lock)
      ✅ Side limit total_trades is atomic & rollback-safe
      ✅ Trades are marked CLOSED (not removed) so PnL stays correct
      ✅ Candle logic is called from main tick_worker on candle-close
    """

    EXIT_BUFFER_PCT = 0.0001
    MAX_TRADES_PER_SYMBOL = 2

    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        stock = state["stocks"].get(token)
        if not stock:
            return

        stock["ltp"] = float(ltp or 0.0)

        mom_status = stock.get("mom_status", "WAITING")

        # monitor open trade regardless of engine toggle
        if mom_status == "OPEN":
            await MomentumEngine.monitor_active_trade(stock, ltp, state)
            return

        if mom_status == "TRIGGER_WATCH":
            side = (stock.get("mom_side_latch") or "").lower()
            if side not in ("mom_bull", "mom_bear"):
                MomentumEngine._reset_waiting(stock)
                return

            if not bool(state["engine_live"].get(side, True)):
                return

            if not MomentumEngine._within_trade_window(state["config"].get(side, {})):
                MomentumEngine._reset_waiting(stock)
                return

            trig = float(stock.get("mom_trigger_px", 0.0) or 0.0)
            if trig <= 0:
                MomentumEngine._reset_waiting(stock)
                return

            if side == "mom_bull" and ltp > trig:
                await MomentumEngine.open_trade(stock, ltp, state, "mom_bull")
            elif side == "mom_bear" and ltp < trig:
                await MomentumEngine.open_trade(stock, ltp, state, "mom_bear")
            return

    # candle-close qualification called by main
    @staticmethod
    async def on_candle_close(token: int, candle: dict, state: dict):
        stock = state["stocks"].get(token)
        if not stock:
            return

        symbol = stock.get("symbol")
        if not symbol:
            return

        if stock.get("mom_status") in ("OPEN", "TRIGGER_WATCH"):
            return

        taken = await TradeControl.get_symbol_trade_count(symbol)
        if taken >= MomentumEngine.MAX_TRADES_PER_SYMBOL:
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

        if close > pdh:
            side = "mom_bull"
            if not bool(state["engine_live"].get(side, True)):
                return
            if not MomentumEngine._within_trade_window(state["config"].get(side, {}), now=now):
                return

            ok, detail = await MomentumEngine.check_vol_matrix(stock, candle, side, state)
            if not ok:
                return

            stock["mom_status"] = "TRIGGER_WATCH"
            stock["mom_side_latch"] = side
            stock["mom_trigger_px"] = float(high)
            stock["mom_trigger_candle"] = dict(candle)

            stock["mom_scan_vol"] = int(c_vol)
            stock["mom_scan_reason"] = f"Momentum candle + Vol OK ({detail})"
            return

        if close < pdl:
            side = "mom_bear"
            if not bool(state["engine_live"].get(side, True)):
                return
            if not MomentumEngine._within_trade_window(state["config"].get(side, {}), now=now):
                return

            ok, detail = await MomentumEngine.check_vol_matrix(stock, candle, side, state)
            if not ok:
                return

            stock["mom_status"] = "TRIGGER_WATCH"
            stock["mom_side_latch"] = side
            stock["mom_trigger_px"] = float(low)
            stock["mom_trigger_candle"] = dict(candle)

            stock["mom_scan_vol"] = int(c_vol)
            stock["mom_scan_reason"] = f"Momentum candle + Vol OK ({detail})"
            return

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

    @staticmethod
    async def open_trade(stock: dict, ltp: float, state: dict, side_key: str):
        symbol = stock.get("symbol") or ""
        if not symbol:
            MomentumEngine._reset_waiting(stock)
            return

        cfg = state["config"].get(side_key, {})
        kite = state.get("kite")
        if not kite:
            MomentumEngine._reset_waiting(stock)
            return

        if not bool(state["engine_live"].get(side_key, True)):
            MomentumEngine._reset_waiting(stock)
            return
        if not MomentumEngine._within_trade_window(cfg):
            MomentumEngine._reset_waiting(stock)
            return

        # side limit reservation
        side_limit = int(cfg.get("total_trades", 5) or 5)
        if not await TradeControl.reserve_side_trade(side_key, side_limit):
            MomentumEngine._reset_waiting(stock)
            return

        # symbol reservation (2/day + open lock)
        ok, reason = await TradeControl.reserve_symbol_trade(symbol, max_trades=MomentumEngine.MAX_TRADES_PER_SYMBOL)
        if not ok:
            await TradeControl.rollback_side_trade(side_key)
            MomentumEngine._reset_waiting(stock)
            return

        is_bull = (side_key == "mom_bull")

        trig_candle = stock.get("mom_trigger_candle") or {}
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
            MomentumEngine._reset_waiting(stock)
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
                "engine": "momentum",
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

            stock["mom_status"] = "OPEN"
            stock["mom_active_trade"] = trade
            stock["mom_side_latch"] = side_key

            stock["mom_scan_seen_ts"] = None
            stock["mom_scan_seen_time"] = None

        except Exception as e:
            logger.error(f"❌ [MOM ORDER ERROR] {symbol}: {e}")
            await TradeControl.rollback_symbol_trade(symbol)
            await TradeControl.rollback_side_trade(side_key)
            MomentumEngine._reset_waiting(stock)

    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        trade = stock.get("mom_active_trade")
        if not trade:
            return

        side_key = (stock.get("mom_side_latch") or "").lower()
        is_bull = (side_key == "mom_bull")

        entry = float(trade.get("entry_price", 0) or 0)
        qty = int(trade.get("qty", 0) or 0)
        sl = float(trade.get("sl_price", 0) or 0)
        target = float(trade.get("target_price", 0) or 0)

        if entry <= 0 or qty <= 0:
            await MomentumEngine.close_position(stock, state, "BAD_TRADE_STATE")
            return

        trade["pnl"] = round((float(ltp) - entry) * qty, 2) if is_bull else round((entry - float(ltp)) * qty, 2)

        b = MomentumEngine.EXIT_BUFFER_PCT
        if is_bull:
            target_hit = float(ltp) >= (target * (1.0 - b))
            sl_hit = float(ltp) <= (sl * (1.0 + b))
        else:
            target_hit = float(ltp) <= (target * (1.0 + b))
            sl_hit = float(ltp) >= (sl * (1.0 - b))

        if target_hit:
            await MomentumEngine.close_position(stock, state, "TARGET")
            return
        if sl_hit:
            await MomentumEngine.close_position(stock, state, "SL")
            return

        new_sl = MomentumEngine._step_trail_sl(trade, float(ltp), is_bull)
        if new_sl is not None:
            cur = float(trade.get("sl_price", 0) or 0)
            if is_bull and new_sl > cur:
                trade["sl_price"] = float(new_sl)
            elif (not is_bull) and new_sl < cur:
                trade["sl_price"] = float(new_sl)

        if stock.get("symbol") in state.get("manual_exits", set()):
            await MomentumEngine.close_position(stock, state, "MANUAL")
            state["manual_exits"].remove(stock["symbol"])

    @staticmethod
    def _step_trail_sl(trade: dict, ltp: float, is_bull: bool):
        entry = float(trade.get("entry_price", 0) or 0)
        step = float(trade.get("trail_step", 0) or 0)
        if entry <= 0 or step <= 0:
            return None

        profit = (ltp - entry) if is_bull else (entry - ltp)
        if profit <= 0:
            return None

        k = int(profit // step)
        if k < 1:
            return None

        return round(entry + ((k - 1) * step), 2) if is_bull else round(entry - ((k - 1) * step), 2)

    @staticmethod
    async def close_position(stock: dict, state: dict, reason: str):
        trade = stock.get("mom_active_trade")
        kite = state.get("kite")
        symbol = stock.get("symbol") or ""
        side_key = (stock.get("mom_side_latch") or "").lower()
        is_bull = (side_key == "mom_bull")

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
                logger.error(f"❌ [MOM EXIT ERROR] {symbol}: {e}")

        if trade:
            trade["status"] = "CLOSED"
            trade["exit_time"] = datetime.now(IST).strftime("%H:%M:%S")
            trade["exit_reason"] = reason

        if symbol:
            await TradeControl.release_symbol_lock(symbol)

        MomentumEngine._reset_waiting(stock)

    @staticmethod
    def _reset_waiting(stock: dict):
        stock["mom_status"] = "WAITING"
        stock["mom_active_trade"] = None

        stock.pop("mom_trigger_px", None)
        stock.pop("mom_side_latch", None)
        stock.pop("mom_trigger_candle", None)

        stock["mom_scan_seen_ts"] = None
        stock["mom_scan_seen_time"] = None
        stock["mom_scan_vol"] = 0
        stock["mom_scan_reason"] = None

    @staticmethod
    def _within_trade_window(cfg: dict, now: Optional[datetime] = None) -> bool:
        from datetime import time as dtime
        try:
            now = now or datetime.now(IST)
            start_s = str(cfg.get("trade_start", "09:15"))
            end_s = str(cfg.get("trade_end", "09:17"))
            sh, sm = map(int, start_s.split(":"))
            eh, em = map(int, end_s.split(":"))
            start_t = dtime(sh, sm)
            end_t = dtime(eh, em)
            nt = now.time()
            return (nt >= start_t) and (nt <= end_t)
        except Exception:
            return True
