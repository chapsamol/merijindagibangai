# momentum_engine.py
import asyncio
import logging
from datetime import datetime, time as dtime
from math import floor
from typing import Optional, Tuple
import pytz

from redis_manager import TradeControl

logger = logging.getLogger("Nexus_Momentum")
IST = pytz.timezone("Asia/Kolkata")


class MomentumEngine:
    """
    Momentum Engine — DHAN integration (architecture unchanged)

    ✅ main.py handles:
      - marketfeed ticks
      - candle aggregation + calling on_candle_close()
      - per-token lock outside
      - OrderUpdate websocket: recomputes entry/target/trailing from AvgTradedPrice

    This module:
      - qualifies only FIRST 1-min candle of day (09:15 bucket)
      - sets trigger watch and opens trade when LTP breaks first high/low
      - places DHAN orders via dhan.place_order in a thread
      - monitors trade (target / SL / trailing), exits via market order
      - uses Redis TradeControl for per-side and per-symbol limits
    """

    EXIT_BUFFER_PCT = 0.0001
    MAX_TRADES_PER_SYMBOL = 2

    DEFAULT_SL_PCT = 0.005  # 0.5%
    FIRST_CANDLE_TIME = dtime(9, 15)

    # -----------------------------
    # TICK FAST-PATH
    # -----------------------------
    @staticmethod
    async def run(token: int, ltp: float, vol: int, state: dict):
        stock = state.get("stocks", {}).get(token)
        if not stock:
            return

        symbol = (stock.get("symbol") or "").strip().upper()
        if not symbol:
            return

        try:
            stock["ltp"] = float(ltp or 0.0)
        except Exception:
            stock["ltp"] = 0.0

        mom_status = (stock.get("mom_status") or "WAITING").upper()

        # 1) Always monitor open trade (even if toggle off)
        if mom_status == "OPEN":
            await MomentumEngine.monitor_active_trade(stock, float(ltp), state)
            return

        # If skip_today is set, do nothing
        if stock.get("mom_skip_today") is True:
            return

        # 2) Trigger watch -> break FIRST candle high/low
        if mom_status == "TRIGGER_WATCH":
            th = float(stock.get("mom_trigger_high", 0.0) or 0.0)
            tl = float(stock.get("mom_trigger_low", 0.0) or 0.0)
            if th <= 0 or tl <= 0:
                return

            cfg_bull = state.get("config", {}).get("mom_bull", {}) or {}
            cfg_bear = state.get("config", {}).get("mom_bear", {}) or {}

            in_bull_window = MomentumEngine._within_trade_window(cfg_bull)
            in_bear_window = MomentumEngine._within_trade_window(cfg_bear)

            if not (in_bull_window or in_bear_window):
                logger.info(f"🕒 [MOM-WINDOW] {symbol} outside both windows; reset.")
                MomentumEngine._reset_waiting(stock)
                return

            px = float(ltp)

            # Break high => LONG
            if px > th:
                if not bool(state.get("engine_live", {}).get("mom_bull", True)):
                    return
                if not in_bull_window:
                    return
                logger.info(f"⚡ [MOM-FIRST-BREAK] {symbol} LONG ltp {px:.2f} > first_high {th:.2f}")
                await MomentumEngine.open_trade(stock, px, state, "mom_bull")
                return

            # Break low => SHORT
            if px < tl:
                if not bool(state.get("engine_live", {}).get("mom_bear", True)):
                    return
                if not in_bear_window:
                    return
                logger.info(f"⚡ [MOM-FIRST-BREAK] {symbol} SHORT ltp {px:.2f} < first_low {tl:.2f}")
                await MomentumEngine.open_trade(stock, px, state, "mom_bear")
                return

        return

    # -----------------------------
    # CANDLE CLOSE (only FIRST candle)
    # -----------------------------
    @staticmethod
    async def on_candle_close(token: int, candle: dict, state: dict):
        stock = state.get("stocks", {}).get(token)
        if not stock:
            return

        symbol = (stock.get("symbol") or "").strip().upper()
        if not symbol:
            return

        # If already open, do nothing
        if (stock.get("mom_status") or "WAITING").upper() == "OPEN":
            return

        # Already captured first candle today?
        today = datetime.now(IST).strftime("%Y%m%d")
        if str(stock.get("mom_first_day") or "") == today:
            return

        # Identify candle bucket time
        bucket = candle.get("bucket")
        bucket_dt: Optional[datetime] = None
        try:
            if isinstance(bucket, datetime):
                bucket_dt = bucket.astimezone(IST) if bucket.tzinfo else bucket.replace(tzinfo=IST)
            elif isinstance(bucket, str) and bucket:
                bucket_dt = datetime.fromisoformat(bucket.replace("Z", "+00:00")).astimezone(IST)
        except Exception:
            bucket_dt = None

        if bucket_dt is None:
            return

        # Only first candle bucket 09:15
        if bucket_dt.time() != MomentumEngine.FIRST_CANDLE_TIME:
            return

        high = float(candle.get("high", 0) or 0)
        low = float(candle.get("low", 0) or 0)
        close = float(candle.get("close", 0) or 0)
        c_vol = int(candle.get("volume", 0) or 0)

        if high <= 0 or low <= 0 or close <= 0:
            return

        prev_close = float(stock.get("prev_close", 0) or 0)
        if prev_close <= 0:
            logger.warning(f"⚠️ [MOM-FIRST] {symbol} prev_close missing/0. Check Redis nexus:market:*")
            return

        # gap filter
        gap_pct = abs((close - prev_close) / prev_close) * 100.0
        if gap_pct > 3.0:
            stock["mom_first_day"] = today
            stock["mom_status"] = "WAITING"
            stock["mom_skip_today"] = True
            stock["mom_skip_reason"] = f"Gap {gap_pct:.2f}% > 3%"
            logger.info(
                f"🚫 [MOM-FIRST-SKIP] {symbol} gap={gap_pct:.2f}% (close={close:.2f}, prev={prev_close:.2f})"
            )
            return

        # Per-symbol cap check (Redis)
        try:
            taken = await TradeControl.get_symbol_trade_count(symbol)
            if int(taken) >= MomentumEngine.MAX_TRADES_PER_SYMBOL:
                stock["mom_first_day"] = today
                stock["mom_skip_today"] = True
                stock["mom_skip_reason"] = f"MAX_TRADES_PER_SYMBOL ({taken})"
                return
        except Exception as e:
            logger.warning(f"[MOM] {symbol} trade_count check failed: {e}")

        # Volume Matrix OR mode on FIRST candle
        cfg_for_matrix = state.get("config", {}).get("mom_bull", {}) or state.get("config", {}).get("mom_bear", {}) or {}
        ok, detail = MomentumEngine.check_vol_matrix_or(stock=stock, candle=candle, cfg=cfg_for_matrix)
        if not ok:
            stock["mom_first_day"] = today
            stock["mom_status"] = "WAITING"
            stock["mom_skip_today"] = True
            stock["mom_skip_reason"] = f"Matrix fail: {detail}"
            logger.info(f"❌ [MOM-FIRST-REJECT] {symbol} Matrix fail: {detail}")
            return

        # Store first candle triggers
        stock["mom_first_day"] = today
        stock["mom_first_candle"] = dict(candle)
        stock["mom_trigger_high"] = float(high)
        stock["mom_trigger_low"] = float(low)

        stock["mom_status"] = "TRIGGER_WATCH"

        stock["mom_scan_vol"] = int(c_vol)
        stock["mom_scan_reason"] = f"First 1m breakout watch | gap={gap_pct:.2f}% | {detail}"
        stock["mom_scan_seen_ts"] = None
        stock["mom_scan_seen_time"] = None

        logger.info(
            f"✅ [MOM-FIRST-SET] {symbol} high={high:.2f} low={low:.2f} close={close:.2f} "
            f"prev={prev_close:.2f} gap={gap_pct:.2f}% | {detail}"
        )

    # -----------------------------
    # VOLUME MATRIX (OR MODE ONLY)
    # -----------------------------
    @staticmethod
    def check_vol_matrix_or(stock: dict, candle: dict, cfg: dict) -> Tuple[bool, str]:
        matrix = (cfg.get("volume_criteria") or []) if isinstance(cfg, dict) else []
        if not matrix:
            return True, "NoMatrix"

        c_vol = int(candle.get("volume", 0) or 0)
        close = float(candle.get("close", 0) or 0)
        s_sma = float(stock.get("sma", 0) or 0)

        if close <= 0 or c_vol <= 0:
            return False, "BadCandleVol/Close"

        turnover_cr = (c_vol * close) / 10000000.0

        best_fail = None
        any_applicable = False

        for i, row in enumerate(matrix):
            if not isinstance(row, dict):
                continue
            try:
                min_sma_avg = float(row.get("min_sma_avg", 0) or 0)
                sma_mult = float(row.get("sma_multiplier", 1.0) or 1.0)
                min_cr = float(row.get("min_vol_price_cr", 0) or 0)
            except Exception:
                continue

            if s_sma < min_sma_avg:
                continue

            any_applicable = True
            req_vol = s_sma * sma_mult

            if (c_vol >= req_vol) and (turnover_cr >= min_cr):
                return True, f"L{i+1} Pass (OR)"

            best_fail = f"L{i+1} Fail (vol {c_vol}<{req_vol:.0f} or cr {turnover_cr:.2f}<{min_cr})"

        if not any_applicable:
            return False, f"NoApplicableRows (SMA={s_sma:.0f})"

        return False, best_fail or "NoRowPassed (OR)"

    # -----------------------------
    # OPEN TRADE (DHAN)
    # -----------------------------
    @staticmethod
    async def open_trade(stock: dict, ltp: float, state: dict, side_key: str):
        symbol = (stock.get("symbol") or "").strip().upper()
        if not symbol:
            MomentumEngine._reset_waiting(stock)
            return

        side_key = (side_key or "").lower().strip()
        if side_key not in ("mom_bull", "mom_bear"):
            logger.error(f"[MOM] {symbol} invalid side_key in open_trade: {side_key}")
            MomentumEngine._reset_waiting(stock)
            return

        cfg = state.get("config", {}).get(side_key, {}) or {}
        dhan = state.get("dhan")
        if not dhan:
            logger.error(f"❌ [MOM] {symbol} dhan session missing")
            MomentumEngine._reset_waiting(stock)
            return

        if not bool(state.get("engine_live", {}).get(side_key, True)):
            MomentumEngine._reset_waiting(stock)
            return

        if not MomentumEngine._within_trade_window(cfg):
            MomentumEngine._reset_waiting(stock)
            return

        # Direction map
        txn_type = dhan.BUY if side_key == "mom_bull" else dhan.SELL

        # 1) reserve side trade
        side_limit = int(cfg.get("total_trades", 5) or 5)
        if not await TradeControl.reserve_side_trade(side_key, side_limit):
            logger.warning(f"🚫 [MOM-LIMIT] {symbol} side limit hit for {side_key}")
            MomentumEngine._reset_waiting(stock)
            return

        # 2) reserve per-symbol lock
        ok, reason = await TradeControl.reserve_symbol_trade(
            symbol,
            max_trades=MomentumEngine.MAX_TRADES_PER_SYMBOL,
            lock_ttl_sec=1800,
        )
        if not ok:
            logger.warning(f"🚫 [MOM-SYMBOL] {symbol} reserve failed: {reason}")
            await TradeControl.rollback_side_trade(side_key)
            MomentumEngine._reset_waiting(stock)
            return

        # Stoploss: percent based from entry (executed-price update will recompute target/trail; SL can remain)
        try:
            sl_pct = float(cfg.get("sl_pct", MomentumEngine.DEFAULT_SL_PCT) or MomentumEngine.DEFAULT_SL_PCT)
        except Exception:
            sl_pct = MomentumEngine.DEFAULT_SL_PCT
        sl_pct = max(0.0005, min(sl_pct, 0.05))

        entry = float(ltp)
        sl_px = round(entry * (1.0 - sl_pct), 2) if side_key == "mom_bull" else round(entry * (1.0 + sl_pct), 2)

        risk_per_share = max(abs(entry - sl_px), entry * 0.0005)
        risk_amount = float(cfg.get("risk_trade_1", 2000) or 2000)
        qty = floor(risk_amount / risk_per_share)

        # Optional hard cap on qty (safety)
        try:
            max_qty = int(cfg.get("max_qty", 0) or 0)
        except Exception:
            max_qty = 0
        if max_qty > 0:
            qty = min(qty, max_qty)

        if qty <= 0:
            await TradeControl.rollback_symbol_trade(symbol)
            await TradeControl.rollback_side_trade(side_key)
            MomentumEngine._reset_waiting(stock)
            return

        # RR / trailing ratios (stored; main.py OrderUpdate recompute uses these)
        try:
            rr_val = float(str(cfg.get("risk_reward", "1:2")).split(":")[-1])
        except Exception:
            rr_val = 2.0

        try:
            tsl_ratio = float(str(cfg.get("trailing_sl", "1:1.5")).split(":")[-1])
        except Exception:
            tsl_ratio = 1.5

        # Provisional target/trailing from LTP (will be recomputed from executed AvgTradedPrice)
        target = round(entry + (risk_per_share * rr_val), 2) if side_key == "mom_bull" else round(entry - (risk_per_share * rr_val), 2)
        trail_step = float(risk_per_share * tsl_ratio) if tsl_ratio > 0 else float(risk_per_share)

        # security_id for Dhan is stored as stock["security_id"] or stock["token"]
        security_id = str(stock.get("security_id") or stock.get("token") or 0)
        if security_id == "0":
            logger.error(f"❌ [MOM] {symbol} security_id missing (token={stock.get('token')})")
            await TradeControl.rollback_symbol_trade(symbol)
            await TradeControl.rollback_side_trade(side_key)
            MomentumEngine._reset_waiting(stock)
            return

        # Exchange segment selection (equity by default)
        exch = cfg.get("exchange_segment")
        exchange_segment = exch if exch else dhan.NSE

        # Place order
        try:
            logger.info(
                f"🧾 [MOM-ORDER] {symbol} {side_key.upper()} "
                f"txn={'BUY' if txn_type==dhan.BUY else 'SELL'} "
                f"secId={security_id} qty={qty} entry(live)={entry:.2f} sl={sl_px:.2f} tgt={target:.2f}"
            )

            resp = await asyncio.to_thread(
                dhan.place_order,
                security_id=str(security_id),
                exchange_segment=exchange_segment,
                transaction_type=txn_type,
                quantity=int(qty),
                order_type=dhan.MARKET,
                product_type=dhan.INTRA,
                price=0
            )

            order_id = MomentumEngine._extract_dhan_order_id(resp)
            if not order_id:
                raise RuntimeError(f"OrderId missing in response: {resp}")

            trade = {
                "engine": "momentum",
                "side": side_key,
                "symbol": symbol,
                "security_id": int(security_id),
                "qty": int(qty),

                # entry_price will be overwritten to executed price when OrderUpdate arrives
                "entry_price": float(entry),
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

                # store ratios for executed recompute
                "rr_val": float(rr_val),
                "tsl_ratio": float(tsl_ratio),

                # store sl_pct for audit
                "sl_pct": float(sl_pct),
            }

            # Ensure list exists
            state.setdefault("trades", {}).setdefault(side_key, [])
            state["trades"][side_key].append(trade)

            stock["mom_status"] = "OPEN"
            stock["mom_active_trade"] = trade
            stock["mom_side_latch"] = side_key

            # Clear scan fields (optional)
            stock["mom_scan_seen_ts"] = None
            stock["mom_scan_seen_time"] = None

            logger.info(f"🚀 [MOM-ENTRY] {symbol} {side_key.upper()} order={order_id} qty={qty}")

        except Exception as e:
            logger.error(f"❌ [MOM-ORDER-FAIL] {symbol}: {e}")
            await TradeControl.rollback_symbol_trade(symbol)
            await TradeControl.rollback_side_trade(side_key)
            MomentumEngine._reset_waiting(stock)

    @staticmethod
    def _extract_dhan_order_id(resp: object) -> Optional[str]:
        """
        dhanhq responses are usually dicts with keys:
          orderId / order_id / OrderId / orderNo / order_no or nested under "data".
        """
        if resp is None:
            return None

        if isinstance(resp, dict):
            for k in ("orderId", "order_id", "orderNo", "order_no", "OrderId", "OrderNo"):
                v = resp.get(k)
                if v:
                    return str(v)

            data = resp.get("data") if isinstance(resp.get("data"), dict) else None
            if data:
                for k in ("orderId", "order_id", "orderNo", "order_no", "OrderId", "OrderNo"):
                    v = data.get(k)
                    if v:
                        return str(v)

        s = str(resp).strip()
        return s if s else None

    # -----------------------------
    # MONITOR + EXIT
    # -----------------------------
    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        trade = stock.get("mom_active_trade")
        if not trade:
            return

        symbol = (stock.get("symbol") or "").strip().upper()
        side_key = (stock.get("mom_side_latch") or "").lower()
        is_bull = (side_key == "mom_bull")

        # entry_price becomes executed price after OrderUpdate recompute
        entry = float(trade.get("entry_price", 0) or 0)
        qty = int(trade.get("qty", 0) or 0)
        sl = float(trade.get("sl_price", 0) or 0)
        target = float(trade.get("target_price", 0) or 0)

        if entry <= 0 or qty <= 0:
            await MomentumEngine.close_position(stock, state, "BAD_TRADE_STATE")
            return

        trade["pnl"] = round(((float(ltp) - entry) * qty) if is_bull else ((entry - float(ltp)) * qty), 2)

        b = MomentumEngine.EXIT_BUFFER_PCT
        if is_bull:
            target_hit = float(ltp) >= (target * (1.0 - b))
            sl_hit = float(ltp) <= (sl * (1.0 + b))
        else:
            target_hit = float(ltp) <= (target * (1.0 + b))
            sl_hit = float(ltp) >= (sl * (1.0 - b))

        if target_hit:
            logger.info(f"🎯 [MOM-TARGET] {symbol} tgt={target:.2f} ltp={ltp:.2f} pnl={trade['pnl']}")
            await MomentumEngine.close_position(stock, state, "TARGET")
            return

        if sl_hit:
            logger.info(f"🛑 [MOM-SL] {symbol} sl={sl:.2f} ltp={ltp:.2f} pnl={trade['pnl']}")
            await MomentumEngine.close_position(stock, state, "SL")
            return

        new_sl = MomentumEngine._step_trail_sl(trade, float(ltp), is_bull)
        if new_sl is not None:
            cur = float(trade.get("sl_price", 0) or 0)
            if is_bull and new_sl > cur:
                trade["sl_price"] = float(new_sl)
            elif (not is_bull) and new_sl < cur:
                trade["sl_price"] = float(new_sl)

        if symbol in state.get("manual_exits", set()):
            logger.info(f"🖱️ [MOM-MANUAL] {symbol}")
            await MomentumEngine.close_position(stock, state, "MANUAL")
            state["manual_exits"].remove(symbol)

    @staticmethod
    def _step_trail_sl(trade: dict, ltp: float, is_bull: bool) -> Optional[float]:
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

        desired = entry + ((k - 1) * step) if is_bull else entry - ((k - 1) * step)
        return round(desired, 2)

    @staticmethod
    async def close_position(stock: dict, state: dict, reason: str):
        trade = stock.get("mom_active_trade")
        dhan = state.get("dhan")

        symbol = (stock.get("symbol") or "").strip().upper()
        side_key = (stock.get("mom_side_latch") or "").lower()
        is_bull = (side_key == "mom_bull")

        txn_type = None
        if dhan:
            txn_type = dhan.SELL if is_bull else dhan.BUY

        security_id = str(stock.get("security_id") or stock.get("token") or 0)

        if trade and dhan and security_id and txn_type:
            try:
                exchange_segment = dhan.NSE
                exit_resp = await asyncio.to_thread(
                    dhan.place_order,
                    security_id=str(security_id),
                    exchange_segment=exchange_segment,
                    transaction_type=txn_type,
                    quantity=int(trade.get("qty", 0) or 0),
                    order_type=dhan.MARKET,
                    product_type=dhan.INTRA,
                    price=0
                )
                exit_id = MomentumEngine._extract_dhan_order_id(exit_resp)
                trade["exit_order_id"] = str(exit_id) if exit_id else None
                logger.info(f"🏁 [MOM-EXIT] {symbol} reason={reason} exit_order={exit_id}")
            except Exception as e:
                logger.error(f"❌ [MOM-EXIT-FAIL] {symbol}: {e}")

        if trade:
            trade["status"] = "CLOSED"
            trade["exit_time"] = datetime.now(IST).strftime("%H:%M:%S")
            trade["exit_reason"] = reason

        if symbol:
            try:
                await TradeControl.release_symbol_lock(symbol)
            except Exception:
                pass

        MomentumEngine._reset_waiting(stock)

    # -----------------------------
    # HELPERS
    # -----------------------------
    @staticmethod
    def _reset_waiting(stock: dict):
        stock["mom_status"] = "WAITING"
        stock["mom_active_trade"] = None
        stock.pop("mom_side_latch", None)

        stock["mom_scan_seen_ts"] = None
        stock["mom_scan_seen_time"] = None
        stock["mom_scan_vol"] = 0
        stock["mom_scan_reason"] = None

        # Do NOT clear first day here; only cleared on next day by date mismatch.
        # Keep skip_today if it was set (prevents repeated triggers for the day).

    @staticmethod
    def _within_trade_window(cfg: dict, now: Optional[datetime] = None) -> bool:
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
