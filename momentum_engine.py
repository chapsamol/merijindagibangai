# momentum_engine.py (ULL-optimized, synced with main.py candle format)
import asyncio
import logging
import time
from datetime import datetime, time as dtime, date as ddate
from math import floor
from typing import Optional, Tuple, Any, Dict
from queue import SimpleQueue
from threading import Thread
import pytz

from redis_manager import TradeControl

logger = logging.getLogger("Nexus_Momentum")
IST = pytz.timezone("Asia/Kolkata")


class _OrderWorker:
    """
    Persistent single thread for synchronous dhan.place_order calls.
    Avoids asyncio.to_thread() overhead + threadpool jitter.
    """
    def __init__(self) -> None:
        self._q: SimpleQueue[Tuple[asyncio.AbstractEventLoop, asyncio.Future, Any, Dict[str, Any]]] = SimpleQueue()
        self._t = Thread(target=self._run, name="dhan_order_worker_mom", daemon=True)
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


class MomentumEngine:
    """
    Momentum Engine — DHAN integration (architecture intact, low-latency hot path)

    ✅ Synced with main.py:
      - expects candle["bucket_epoch"] (epoch seconds bucket start)
      - uses state["stocks"][token] dict

    ✅ Low latency:
      - run() never awaits order/redis; schedules async tasks (OPENING/CLOSING)
      - persistent order worker thread (no asyncio.to_thread per order)
      - cached trade-window parsing

    ✅ Dependencies intact:
      - TradeControl reserve/rollback/release
      - dhan.place_order
      - trade dict schema matches your order-update recompute logic
    """

    EXIT_BUFFER_PCT = 0.0001
    MAX_TRADES_PER_SYMBOL = 2

    DEFAULT_SL_PCT = 0.005  # 0.5%
    FIRST_CANDLE_TIME = dtime(9, 15)
    TRIGGER_VALID_SECONDS = 6 * 60  # optional TTL for trigger watch

    _order_worker: Optional[_OrderWorker] = None
    _tw_cache: Dict[Tuple[str, str], Tuple[Any, Any]] = {}

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
        stock = state.get("stocks", {}).get(token)
        if not stock:
            return

        symbol = (stock.get("symbol") or "").strip().upper()
        if not symbol:
            return

        px = float(ltp or 0.0)
        stock["ltp"] = px

        mom_status = (stock.get("mom_status") or "WAITING").upper()

        # Always monitor open trade (even if toggle off)
        if mom_status == "OPEN":
            # monitor may schedule exit; keep it tick-fast
            await MomentumEngine.monitor_active_trade(stock, px, state)
            return

        # OPENING/CLOSING are inflight; don't do anything else
        if mom_status in ("OPENING", "CLOSING"):
            return

        # Skip today?
        if stock.get("mom_skip_today") is True:
            return

        # Trigger watch => break FIRST candle high/low
        if mom_status != "TRIGGER_WATCH":
            return

        # TTL check (cheap)
        set_ts = int(stock.get("mom_trigger_set_ts") or 0)
        if set_ts:
            now_ts = int(time.time())
            if (now_ts - set_ts) > MomentumEngine.TRIGGER_VALID_SECONDS:
                await MomentumEngine._rollback_watch_reservations(stock)
                MomentumEngine._reset_waiting(stock)
                return

        th = float(stock.get("mom_trigger_high", 0.0) or 0.0)
        tl = float(stock.get("mom_trigger_low", 0.0) or 0.0)
        if th <= 0.0 or tl <= 0.0:
            return

        cfg_bull = state.get("config", {}).get("mom_bull", {}) or {}
        cfg_bear = state.get("config", {}).get("mom_bear", {}) or {}

        in_bull_window = MomentumEngine._within_trade_window(cfg_bull)
        in_bear_window = MomentumEngine._within_trade_window(cfg_bear)

        if not (in_bull_window or in_bear_window):
            await MomentumEngine._rollback_watch_reservations(stock)
            MomentumEngine._reset_waiting(stock)
            return

        # Break high => LONG
        if px > th:
            if not bool(state.get("engine_live", {}).get("mom_bull", True)):
                return
            if not in_bull_window:
                return

            # schedule entry task (no blocking in dispatcher)
            stock["mom_status"] = "OPENING"
            stock["mom_side_latch"] = "mom_bull"
            asyncio.create_task(MomentumEngine.open_trade(stock, px, state, "mom_bull"))
            return

        # Break low => SHORT
        if px < tl:
            if not bool(state.get("engine_live", {}).get("mom_bear", True)):
                return
            if not in_bear_window:
                return

            stock["mom_status"] = "OPENING"
            stock["mom_side_latch"] = "mom_bear"
            asyncio.create_task(MomentumEngine.open_trade(stock, px, state, "mom_bear"))
            return

    # -----------------------------
    # CANDLE CLOSE (only FIRST candle 09:15 IST)
    # -----------------------------
    @staticmethod
    async def on_candle_close(token: int, candle: dict, state: dict):
        stock = state.get("stocks", {}).get(token)
        if not stock:
            return

        symbol = (stock.get("symbol") or "").strip().upper()
        if not symbol:
            return

        # If already open/inflight, do nothing
        st = (stock.get("mom_status") or "WAITING").upper()
        if st in ("OPEN", "OPENING", "CLOSING"):
            return

        # Already captured first candle today?
        today_str = datetime.now(IST).strftime("%Y%m%d")
        if str(stock.get("mom_first_day") or "") == today_str:
            return

        # ---- SYNC WITH main.py ----
        # main.py sends bucket_epoch (start epoch seconds)
        bucket_epoch = int(candle.get("bucket_epoch") or 0)
        if bucket_epoch <= 0:
            return

        # Confirm this candle is the FIRST candle bucket (09:15 IST) of today
        # Compute today's 09:15 bucket epoch in IST
        dt_today = datetime.now(IST).date()
        dt_first = IST.localize(datetime(dt_today.year, dt_today.month, dt_today.day,
                                        MomentumEngine.FIRST_CANDLE_TIME.hour,
                                        MomentumEngine.FIRST_CANDLE_TIME.minute, 0))
        first_bucket_epoch = int(dt_first.timestamp())

        if bucket_epoch != first_bucket_epoch:
            return

        high = float(candle.get("high", 0) or 0.0)
        low = float(candle.get("low", 0) or 0.0)
        close = float(candle.get("close", 0) or 0.0)
        c_vol = int(candle.get("volume", 0) or 0)

        if high <= 0.0 or low <= 0.0 or close <= 0.0:
            return

        prev_close = float(stock.get("prev_close", 0) or 0.0)
        if prev_close <= 0.0:
            # You should load prev_close into ENGINE_STATE["stocks"] at startup (see main.py patch below)
            return

        # Gap filter
        gap_pct = abs((close - prev_close) / prev_close) * 100.0
        if gap_pct > 3.0:
            stock["mom_first_day"] = today_str
            stock["mom_status"] = "WAITING"
            stock["mom_skip_today"] = True
            stock["mom_skip_reason"] = f"Gap {gap_pct:.2f}% > 3%"
            return

        # Per-symbol cap check (Redis) — candle-close path only
        try:
            taken = await TradeControl.get_symbol_trade_count(symbol)
            if int(taken) >= MomentumEngine.MAX_TRADES_PER_SYMBOL:
                stock["mom_first_day"] = today_str
                stock["mom_skip_today"] = True
                stock["mom_skip_reason"] = f"MAX_TRADES_PER_SYMBOL ({taken})"
                return
        except Exception:
            pass

        # Volume Matrix OR mode on FIRST candle
        cfg_for_matrix = (
            state.get("config", {}).get("mom_bull", {}) or
            state.get("config", {}).get("mom_bear", {}) or {}
        )
        ok, detail = MomentumEngine.check_vol_matrix_or(stock=stock, candle=candle, cfg=cfg_for_matrix)
        if not ok:
            stock["mom_first_day"] = today_str
            stock["mom_status"] = "WAITING"
            stock["mom_skip_today"] = True
            stock["mom_skip_reason"] = f"Matrix fail: {detail}"
            return

        # Optional: reserve SYMBOL now (off hot path). Side reserve happens in async open_trade task.
        reserved = await MomentumEngine._reserve_symbol_for_watch(symbol, stock)
        if not reserved:
            stock["mom_first_day"] = today_str
            stock["mom_skip_today"] = True
            stock["mom_skip_reason"] = "Symbol reserve failed"
            return

        # Store first candle triggers
        stock["mom_first_day"] = today_str
        stock["mom_first_candle"] = dict(candle)
        stock["mom_trigger_high"] = float(high)
        stock["mom_trigger_low"] = float(low)
        stock["mom_trigger_set_ts"] = int(time.time())

        stock["mom_status"] = "TRIGGER_WATCH"

        stock["mom_scan_vol"] = int(c_vol)
        stock["mom_scan_reason"] = f"First 1m breakout watch | gap={gap_pct:.2f}% | {detail}"
        stock["mom_scan_seen_ts"] = None
        stock["mom_scan_seen_time"] = None

    # -----------------------------
    # VOLUME MATRIX (OR MODE ONLY)
    # -----------------------------
    @staticmethod
    def check_vol_matrix_or(stock: dict, candle: dict, cfg: dict) -> Tuple[bool, str]:
        matrix = (cfg.get("volume_criteria") or []) if isinstance(cfg, dict) else []
        if not matrix:
            return True, "NoMatrix"

        c_vol = int(candle.get("volume", 0) or 0)
        close = float(candle.get("close", 0) or 0.0)
        s_sma = float(stock.get("sma", 0) or 0.0)

        if close <= 0.0 or c_vol <= 0:
            return False, "BadCandleVol/Close"

        turnover_cr = (c_vol * close) / 10000000.0

        best_fail = None
        any_applicable = False

        for i, row in enumerate(matrix):
            if not isinstance(row, dict):
                continue
            try:
                min_sma_avg = float(row.get("min_sma_avg", 0) or 0.0)
                sma_mult = float(row.get("sma_multiplier", 1.0) or 1.0)
                min_cr = float(row.get("min_vol_price_cr", 0) or 0.0)
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
    # RESERVATIONS (moved off hot tick path)
    # -----------------------------
    @staticmethod
    async def _reserve_symbol_for_watch(symbol: str, stock: dict) -> bool:
        if bool(stock.get("mom_reserved_symbol_watch")):
            return True
        try:
            ok, _reason = await TradeControl.reserve_symbol_trade(
                symbol,
                max_trades=MomentumEngine.MAX_TRADES_PER_SYMBOL,
                lock_ttl_sec=1800,
            )
            if ok:
                stock["mom_reserved_symbol_watch"] = True
                stock["mom_reserved_symbol"] = symbol
                return True
            return False
        except Exception:
            return False

    @staticmethod
    async def _rollback_watch_reservations(stock: dict) -> None:
        symbol = stock.get("mom_reserved_symbol") or ""
        if symbol and bool(stock.get("mom_reserved_symbol_watch")):
            try:
                await TradeControl.rollback_symbol_trade(symbol)
            except Exception:
                pass
        stock.pop("mom_reserved_symbol_watch", None)
        stock.pop("mom_reserved_symbol", None)

    # -----------------------------
    # OPEN TRADE (DHAN) — runs as background task
    # -----------------------------
    @staticmethod
    async def open_trade(stock: dict, ltp: float, state: dict, side_key: str):
        symbol = (stock.get("symbol") or "").strip().upper()
        if not symbol:
            await MomentumEngine._rollback_watch_reservations(stock)
            MomentumEngine._reset_waiting(stock)
            return

        side_key = (side_key or "").lower().strip()
        if side_key not in ("mom_bull", "mom_bear"):
            await MomentumEngine._rollback_watch_reservations(stock)
            MomentumEngine._reset_waiting(stock)
            return

        cfg = state.get("config", {}).get(side_key, {}) or {}
        dhan = state.get("dhan")
        if not dhan:
            await MomentumEngine._rollback_watch_reservations(stock)
            MomentumEngine._reset_waiting(stock)
            return

        if not bool(state.get("engine_live", {}).get(side_key, True)):
            await MomentumEngine._rollback_watch_reservations(stock)
            MomentumEngine._reset_waiting(stock)
            return

        if not MomentumEngine._within_trade_window(cfg):
            await MomentumEngine._rollback_watch_reservations(stock)
            MomentumEngine._reset_waiting(stock)
            return

        txn_type = dhan.BUY if side_key == "mom_bull" else dhan.SELL

        # Reserve side trade HERE (async task, not in tick dispatcher lock)
        side_limit = int(cfg.get("total_trades", 5) or 5)
        if not await TradeControl.reserve_side_trade(side_key, side_limit):
            await MomentumEngine._rollback_watch_reservations(stock)
            MomentumEngine._reset_waiting(stock)
            return

        # Ensure symbol lock exists (if watch reserve was not taken, take now)
        if not bool(stock.get("mom_reserved_symbol_watch")):
            ok_sym = await MomentumEngine._reserve_symbol_for_watch(symbol, stock)
            if not ok_sym:
                await TradeControl.rollback_side_trade(side_key)
                MomentumEngine._reset_waiting(stock)
                return

        # SL percent
        try:
            sl_pct = float(cfg.get("sl_pct", MomentumEngine.DEFAULT_SL_PCT) or MomentumEngine.DEFAULT_SL_PCT)
        except Exception:
            sl_pct = MomentumEngine.DEFAULT_SL_PCT
        sl_pct = max(0.0005, min(sl_pct, 0.05))

        entry = float(ltp or 0.0)
        sl_px = round(entry * (1.0 - sl_pct), 2) if side_key == "mom_bull" else round(entry * (1.0 + sl_pct), 2)

        risk_per_share = max(abs(entry - sl_px), entry * 0.0005)
        risk_amount = float(cfg.get("risk_trade_1", 2000) or 2000.0)
        qty = floor(risk_amount / risk_per_share) if risk_per_share > 0 else 0

        try:
            max_qty = int(cfg.get("max_qty", 0) or 0)
        except Exception:
            max_qty = 0
        if max_qty > 0:
            qty = min(qty, max_qty)

        if qty <= 0:
            await MomentumEngine._rollback_watch_reservations(stock)
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

        target = round(entry + (risk_per_share * rr_val), 2) if side_key == "mom_bull" else round(entry - (risk_per_share * rr_val), 2)
        trail_step = float(risk_per_share * tsl_ratio) if tsl_ratio > 0 else float(risk_per_share)

        security_id = str(int(stock.get("security_id") or stock.get("token") or 0))
        if security_id == "0":
            await MomentumEngine._rollback_watch_reservations(stock)
            await TradeControl.rollback_side_trade(side_key)
            MomentumEngine._reset_waiting(stock)
            return

        exchange_segment = cfg.get("exchange_segment") or dhan.NSE

        try:
            resp = await MomentumEngine._place_order(
                dhan,
                security_id=str(security_id),
                exchange_segment=exchange_segment,
                transaction_type=txn_type,
                quantity=int(qty),
                order_type=dhan.MARKET,
                product_type=dhan.INTRA,
                price=0,
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

                "entry_price": float(entry),      # will be overwritten by executed avg price later
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

                "sl_pct": float(sl_pct),
            }

            state.setdefault("trades", {}).setdefault(side_key, [])
            state["trades"][side_key].append(trade)

            stock["mom_status"] = "OPEN"
            stock["mom_active_trade"] = trade
            stock["mom_side_latch"] = side_key

            # consume watch symbol reservation (trade is open now; release happens on close_position)
            stock.pop("mom_reserved_symbol_watch", None)
            stock.pop("mom_reserved_symbol", None)

            stock["mom_scan_seen_ts"] = None
            stock["mom_scan_seen_time"] = None

        except Exception:
            # rollback reservations
            await MomentumEngine._rollback_watch_reservations(stock)
            try:
                await TradeControl.rollback_side_trade(side_key)
            except Exception:
                pass
            MomentumEngine._reset_waiting(stock)

    @staticmethod
    async def _place_order(dhan: Any, **kwargs: Any) -> Any:
        worker = MomentumEngine._get_order_worker()
        fut = worker.submit(dhan.place_order, **kwargs)
        return await fut

    @staticmethod
    def _extract_dhan_order_id(resp: object) -> Optional[str]:
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
    # MONITOR + EXIT (tick-fast, schedules exit)
    # -----------------------------
    @staticmethod
    async def monitor_active_trade(stock: dict, ltp: float, state: dict):
        trade = stock.get("mom_active_trade")
        if not trade:
            return

        side_key = (stock.get("mom_side_latch") or "").lower()
        if side_key not in ("mom_bull", "mom_bear"):
            stock["mom_status"] = "CLOSING"
            asyncio.create_task(MomentumEngine.close_position(stock, state, "BAD_SIDE_LATCH"))
            return

        is_bull = (side_key == "mom_bull")

        entry = float(trade.get("entry_price", 0) or 0.0)
        qty = int(trade.get("qty", 0) or 0)
        sl = float(trade.get("sl_price", 0) or 0.0)
        target = float(trade.get("target_price", 0) or 0.0)

        if entry <= 0.0 or qty <= 0:
            stock["mom_status"] = "CLOSING"
            asyncio.create_task(MomentumEngine.close_position(stock, state, "BAD_TRADE_STATE"))
            return

        px = float(ltp or 0.0)
        trade["pnl"] = round(((px - entry) * qty) if is_bull else ((entry - px) * qty), 2)

        b = MomentumEngine.EXIT_BUFFER_PCT
        if is_bull:
            target_hit = px >= (target * (1.0 - b))
            sl_hit = px <= (sl * (1.0 + b))
        else:
            target_hit = px <= (target * (1.0 + b))
            sl_hit = px >= (sl * (1.0 - b))

        if target_hit:
            stock["mom_status"] = "CLOSING"
            asyncio.create_task(MomentumEngine.close_position(stock, state, "TARGET"))
            return

        if sl_hit:
            stock["mom_status"] = "CLOSING"
            asyncio.create_task(MomentumEngine.close_position(stock, state, "SL"))
            return

        new_sl = MomentumEngine._step_trail_sl(trade, px, is_bull)
        if new_sl is not None:
            cur = float(trade.get("sl_price", 0) or 0.0)
            if is_bull and new_sl > cur:
                trade["sl_price"] = float(new_sl)
            elif (not is_bull) and new_sl < cur:
                trade["sl_price"] = float(new_sl)

        symbol = (stock.get("symbol") or "").strip().upper()
        if symbol and symbol in state.get("manual_exits", set()):
            stock["mom_status"] = "CLOSING"
            state["manual_exits"].remove(symbol)
            asyncio.create_task(MomentumEngine.close_position(stock, state, "MANUAL"))

    @staticmethod
    def _step_trail_sl(trade: dict, ltp: float, is_bull: bool) -> Optional[float]:
        entry = float(trade.get("entry_price", 0) or 0.0)
        step = float(trade.get("trail_step", 0) or 0.0)
        if entry <= 0.0 or step <= 0.0:
            return None

        profit = (ltp - entry) if is_bull else (entry - ltp)
        if profit <= 0.0:
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

        security_id = str(int(stock.get("security_id") or stock.get("token") or 0))
        exchange_segment = dhan.NSE if dhan else None

        if trade and dhan and security_id != "0" and txn_type and exchange_segment:
            try:
                resp = await MomentumEngine._place_order(
                    dhan,
                    security_id=str(security_id),
                    exchange_segment=exchange_segment,
                    transaction_type=txn_type,
                    quantity=int(trade.get("qty", 0) or 0),
                    order_type=dhan.MARKET,
                    product_type=dhan.INTRA,
                    price=0,
                )
                exit_id = MomentumEngine._extract_dhan_order_id(resp)
                trade["exit_order_id"] = str(exit_id) if exit_id else None
            except Exception:
                pass

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
        # do not clear mom_first_day or mom_skip_today here

        # clear trigger + watch data
        stock.pop("mom_trigger_high", None)
        stock.pop("mom_trigger_low", None)
        stock.pop("mom_trigger_set_ts", None)

        # clear watch reservation flags (actual rollback is explicit)
        stock.pop("mom_reserved_symbol_watch", None)
        stock.pop("mom_reserved_symbol", None)

    @staticmethod
    def _within_trade_window(cfg: dict, now: Optional[datetime] = None) -> bool:
        """
        Cached parsing of start/end times to avoid repeated split/int conversion.
        """
        from datetime import time as _time
        try:
            now = now or datetime.now(IST)
            start_s = str(cfg.get("trade_start", "09:15"))
            end_s = str(cfg.get("trade_end", "15:10"))
            key = (start_s, end_s)

            cached = MomentumEngine._tw_cache.get(key)
            if cached is None:
                sh, sm = map(int, start_s.split(":"))
                eh, em = map(int, end_s.split(":"))
                cached = (_time(sh, sm), _time(eh, em))
                MomentumEngine._tw_cache[key] = cached

            start_t, end_t = cached
            nt = now.time()
            return (nt >= start_t) and (nt <= end_t)
        except Exception:
            return True
