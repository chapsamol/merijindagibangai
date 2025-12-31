# -*- coding: utf-8 -*-
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
    Breakout Engine (Bull/Bear) — Dhan Orders

    - run() is tick-fast
    - on_candle_close() is called by main.py candle worker
    - per-symbol lock is handled outside (main.py token_locks)
    - Redis reservations unchanged
    - Orders via dhan.place_order(...)
    - token == security_id
    - Target/Trailing are recomputed from executed AvgTradedPrice via OrderUpdate in main.py
    """

    EXIT_BUFFER_PCT = 0.0001
    TRIGGER_VALID_SECONDS = 6 * 60
    MAX_TRADES_PER_SYMBOL = 2

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

        stock["ltp"] = float(ltp or 0.0)
        brk_status = (stock.get("brk_status") or "WAITING").upper()

        # Always monitor open trade even if engine toggle off
        if brk_status == "OPEN":
            await BreakoutEngine.monitor_active_trade(stock, float(ltp), state)
            return

        if brk_status != "TRIGGER_WATCH":
            return

        side = (stock.get("brk_side_latch") or "").lower()
        if side not in ("bull", "bear"):
            logger.warning(f"[BRK] {symbol} invalid side_latch; resetting.")
            BreakoutEngine._reset_waiting(stock)
            return

        # TTL check
        now_ts = int(datetime.now(IST).timestamp())
        set_ts = int(stock.get("brk_trigger_set_ts") or 0)
        if set_ts and (now_ts - set_ts) > BreakoutEngine.TRIGGER_VALID_SECONDS:
            logger.info(f"⏳ [BRK-EXPIRE] {symbol} {side.upper()} trigger expired (>6m). Reset.")
            BreakoutEngine._reset_waiting(stock)
            return

        # Engine toggle gates new entry only
        if not bool(state["engine_live"].get(side, True)):
            return

        if not BreakoutEngine._within_trade_window(state["config"].get(side, {})):
            logger.info(f"🕒 [BRK-WINDOW] {symbol} {side.upper()} outside trade window; reset.")
            BreakoutEngine._reset_waiting(stock)
            return

        trig = float(stock.get("brk_trigger_px", 0.0) or 0.0)
        if trig <= 0:
            BreakoutEngine._reset_waiting(stock)
            return

        px = float(ltp)
        if side == "bull" and px > trig:
            logger.info(f"⚡ [BRK-TRIGGER] {symbol} BULL ltp {px:.2f} > {trig:.2f}")
            await BreakoutEngine.open_trade(stock, px, state, "bull")
        elif side == "bear" and px < trig:
            logger.info(f"⚡ [BRK-TRIGGER] {symbol} BEAR ltp {px:.2f} < {trig:.2f}")
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

        if (stock.get("brk_status") or "WAITING").upper() in ("OPEN", "TRIGGER_WATCH"):
            return

        # per-symbol cap check
        try:
            taken = await TradeControl.get_symbol_trade_count(symbol)
            if int(taken) >= BreakoutEngine.MAX_TRADES_PER_SYMBOL:
                return
        except Exception as e:
            logger.warning(f"[BRK] {symbol} trade_count check failed: {e}")

        pdh = float(stock.get("pdh", 0) or 0)
        pdl = float(stock.get("pdl", 0) or 0)
        if pdh <= 0 or pdl <= 0:
            return

        c_open = float(candle.get("open", 0))
        high = float(candle.get("high", 0) or 0)
        low = float(candle.get("low", 0) or 0)
        close = float(candle.get("close", 0) or 0)
        c_vol = int(candle.get("volume", 0) or 0)
        if close <= 0 or high <= 0 or low <= 0:
            return

        now = datetime.now(IST)

        # --- BULL BREAKOUT ---
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

            stock["brk_status"] = "TRIGGER_WATCH"
            stock["brk_side_latch"] = "bull"
            stock["brk_trigger_px"] = float(high)
            stock["brk_trigger_set_ts"] = int(now.timestamp())
            stock["brk_trigger_candle"] = dict(candle)

            stock["brk_scan_vol"] = int(c_vol)
            stock["brk_scan_reason"] = f"PDH break + Vol OK ({detail})"
            stock["brk_scan_seen_ts"] = None
            stock["brk_scan_seen_time"] = None
            logger.info(f"✅ [BRK-QUALIFIED] {symbol} BULL trigger@{high:.2f} {detail}")
            return

        # --- BEAR BREAKDOWN ---
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

            stock["brk_status"] = "TRIGGER_WATCH"
            stock["brk_side_latch"] = "bear"
            stock["brk_trigger_px"] = float(low)
            stock["brk_trigger_set_ts"] = int(now.timestamp())
            stock["brk_trigger_candle"] = dict(candle)

            stock["brk_scan_vol"] = int(c_vol)
            stock["brk_scan_reason"] = f"PDL break + Vol OK ({detail})"
            stock["brk_scan_seen_ts"] = None
            stock["brk_scan_seen_time"] = None
            logger.info(f"✅ [BRK-QUALIFIED] {symbol} BEAR trigger@{low:.2f} {detail}")
            return

    @staticmethod
    async def check_vol_matrix(stock: dict, candle: dict, side: str, state: dict):
        cfg = state["config"].get(side, {})
        matrix = cfg.get("volume_criteria", []) or []

        c_vol = int(candle.get("volume", 0) or 0)
        s_sma = float(stock.get("sma", 0) or 0)
        close = float(candle.get("close", 0) or 0)
        turnover_cr = (c_vol * close) / 10000000.0 if close > 0 else 0.0

        if not matrix:
            return True, "NoMatrix"

        best_fail = None
        for i, level in enumerate(matrix):
            try:
                min_sma_avg = float(level.get("min_sma_avg", 0) or 0)
                sma_mult = float(level.get("sma_multiplier", 1.0) or 1.0)
                min_cr = float(level.get("min_vol_price_cr", 0) or 0)
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
            BreakoutEngine._reset_waiting(stock)
            return

        side_key = side_key.lower().strip()
        if side_key not in ("bull", "bear"):
            BreakoutEngine._reset_waiting(stock)
            return

        cfg = state["config"].get(side_key, {}) or {}
        dhan = state.get("dhan")
        if not dhan:
            logger.error(f"❌ [BRK] {symbol} dhan session missing")
            BreakoutEngine._reset_waiting(stock)
            return

        if not bool(state["engine_live"].get(side_key, True)):
            BreakoutEngine._reset_waiting(stock)
            return

        if not BreakoutEngine._within_trade_window(cfg):
            BreakoutEngine._reset_waiting(stock)
            return

        txn_type = dhan.BUY if side_key == "bull" else dhan.SELL

        # -------------------- reservations --------------------
        side_limit = int(cfg.get("total_trades", 5) or 5)

        if not await TradeControl.reserve_side_trade(side_key, side_limit):
            BreakoutEngine._reset_waiting(stock)
            return

        ok, _reason = await TradeControl.reserve_symbol_trade(symbol, max_trades=BreakoutEngine.MAX_TRADES_PER_SYMBOL)
        if not ok:
            await TradeControl.rollback_side_trade(side_key)
            BreakoutEngine._reset_waiting(stock)
            return

        # -------------------- SL derived from trigger candle size --------------------
        trig_candle = stock.get("brk_trigger_candle") or {}
        entry = float(ltp)

        tc_high = float(trig_candle.get("high", 0) or 0)
        tc_low = float(trig_candle.get("low", 0) or 0)
        tc_close = float(trig_candle.get("close", 0) or 0) or entry

        range_pct = ((tc_high - tc_low) / tc_close) * 100.0 if tc_close > 0 else 0.0
        pdh = float(stock.get("pdh", 0) or 0)
        pdl = float(stock.get("pdl", 0) or 0)

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
        risk_amount = float(cfg.get("risk_trade_1", 2000) or 2000)
        qty = floor(risk_amount / risk_per_share) if risk_per_share > 0 else 0

        if qty <= 0:
            await TradeControl.rollback_symbol_trade(symbol)
            await TradeControl.rollback_side_trade(side_key)
            BreakoutEngine._reset_waiting(stock)
            return

        # ratios for executed-price recompute in main.py
        try:
            rr_val = float(str(cfg.get("risk_reward", "1:2")).split(":")[-1])
        except Exception:
            rr_val = 2.0

        try:
            tsl_ratio = float(str(cfg.get("trailing_sl", "1:1.5")).split(":")[-1])
        except Exception:
            tsl_ratio = 1.5

        target = round(entry + (risk_per_share * rr_val), 2) if side_key == "bull" else round(entry - (risk_per_share * rr_val), 2)
        trail_step = float(risk_per_share * tsl_ratio) if tsl_ratio > 0 else float(risk_per_share)

        security_id = str(int(stock.get("security_id") or stock.get("token") or 0))
        if security_id == "0":
            await TradeControl.rollback_symbol_trade(symbol)
            await TradeControl.rollback_side_trade(side_key)
            BreakoutEngine._reset_waiting(stock)
            return

        try:
            logger.info(
                f"🧾 [BRK-ORDER] {symbol} {side_key.upper()} "
                f"secId={security_id} qty={qty} entry(live)={entry:.2f} sl={sl_px:.2f} tgt={target:.2f}"
            )

            resp = await asyncio.to_thread(
                dhan.place_order,
                security_id=security_id,
                exchange_segment=dhan.NSE,
                transaction_type=txn_type,
                quantity=int(qty),
                order_type=dhan.MARKET,
                product_type=dhan.INTRA,
                price=0
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

            stock.pop("brk_trigger_px", None)
            stock["brk_trigger_set_ts"] = None
            stock["brk_scan_seen_ts"] = None
            stock["brk_scan_seen_time"] = None

            logger.info(f"🚀 [BRK-ENTRY] {symbol} {side_key.upper()} order={order_id} qty={qty}")

        except Exception as e:
            logger.error(f"❌ [BRK-ORDER-FAIL] {symbol}: {e}")
            await TradeControl.rollback_symbol_trade(symbol)
            await TradeControl.rollback_side_trade(side_key)
            BreakoutEngine._reset_waiting(stock)

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
                logger.info(f"🏁 [BRK-EXIT] {symbol} reason={reason} qty={trade.get('qty')}")
                resp = await asyncio.to_thread(
                    dhan.place_order,
                    security_id=security_id,
                    exchange_segment=dhan.NSE,
                    transaction_type=txn_type,
                    quantity=int(trade["qty"]),
                    order_type=dhan.MARKET,
                    product_type=dhan.INTRA,
                    price=0
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
