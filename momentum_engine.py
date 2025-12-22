import logging
from datetime import datetime, time as dt_time
from math import floor
import pytz
from redis_manager import TradeControl

logger = logging.getLogger("Nexus_Momentum")
IST = pytz.timezone("Asia/Kolkata")

class MomentumEngine:
    @staticmethod
    def run(token, ltp, vol, state):
        stock = state["stocks"].get(token)
        if not stock or stock['symbol'] in state.get("banned", set()): 
            return
            
        now_dt = datetime.now(IST)
        now_time = now_dt.time()

        # 1. Monitor Active Momentum Trades
        if stock.get('status') == 'OPEN_MOM':
            MomentumEngine.monitor_active_trade(stock, ltp, state)
            return

        # 2. 9:15 - 9:16: Range Formation (High/Low of first minute)
        if dt_time(9, 15) <= now_time < dt_time(9, 16):
            stock['hi'] = max(stock['hi'], ltp) if stock['hi'] > 0 else ltp
            stock['lo'] = min(stock['lo'], ltp) if stock['lo'] > 0 else ltp
            
            if not stock.get('candle_915'): 
                stock['candle_915'] = {'volume': 0, 'close': ltp}
            
            stock['candle_915']['close'] = ltp
            if stock['last_vol'] > 0:
                stock['candle_915']['volume'] += max(0, vol - stock['last_vol'])
            
            stock['last_vol'] = vol
            return

        # 3. Post 9:16: Trigger Logic
        if stock.get('status') == 'WAITING' and now_time >= dt_time(9, 16):
            # BULL TRIGGER
            if state["engine_live"]["mom_bull"] and ltp > (stock['hi'] * 1.0001):
                if MomentumEngine.is_vol_qualified(stock, 'mom_bull', state):
                    # Atomic Limit Check using Redis LUA
                    limit = state["config"]["mom_bull"]["total_trades"]
                    if TradeControl.can_trade("mom_bull", limit):
                        MomentumEngine.open_trade("mom_bull", stock, ltp, state)
            
            # BEAR TRIGGER
            elif state["engine_live"]["mom_bear"] and ltp < (stock['lo'] * 0.9999):
                if MomentumEngine.is_vol_qualified(stock, 'mom_bear', state):
                    limit = state["config"]["mom_bear"]["total_trades"]
                    if TradeControl.can_trade("mom_bear", limit):
                        MomentumEngine.open_trade("mom_bear", stock, ltp, state)
        
        stock['last_vol'] = vol

    @staticmethod
    def is_vol_qualified(stock, side, state):
        c = stock.get('candle_915')
        if not c: return False
        
        matrix = state["config"][side].get('volume_criteria', [])
        if not matrix: return True
        
        c_vol = c['volume']
        c_val_cr = (c_vol * c['close']) / 10000000.0
        s_sma = stock.get('sma', 0)
        
        for level in matrix:
            try:
                min_sma = float(level.get('min_sma_avg', 0))
                sma_mult = float(level.get('sma_multiplier', 1))
                min_cr = float(level.get('min_vol_price_cr', 0))
                
                if s_sma >= min_sma and c_vol >= (s_sma * sma_mult) and c_val_cr >= min_cr:
                    return True
            except: continue
        return False

    @staticmethod
    def open_trade(side, stock, price, state):
        cfg = state["config"][side]
        sl_pct = float(cfg.get('stop_loss_pct', 0.5)) / 100.0
        risk_amt = float(cfg.get('risk_per_trade', 2000))
        
        qty = max(1, int(floor(risk_amt / (price * sl_pct))))
        rr_val = float(str(cfg.get('risk_reward', '1:2')).split(':')[1])
        
        is_bull = "bull" in side
        sl_price = price * (1 - sl_pct) if is_bull else price * (1 + sl_pct)
        target_price = price * (1 + (sl_pct * rr_val)) if is_bull else price * (1 - (sl_pct * rr_val))
        
        trade_obj = {
            "symbol": stock['symbol'],
            "entry_price": price,
            "ltp": price,
            "sl_price": sl_price,
            "target_price": target_price,
            "qty": qty,
            "side": "BUY" if is_bull else "SELL",
            "opened_at": datetime.now(IST).strftime("%H:%M:%S")
        }
        
        state["trades"][side].append(trade_obj)
        stock['status'] = 'OPEN_MOM'
        logger.info(f"MOMENTUM TRADE OPENED: {stock['symbol']} Qty: {qty}")

    @staticmethod
    def monitor_active_trade(stock, ltp, state):
        for side in ["mom_bull", "mom_bear"]:
            state["trades"][side] = [t for t in state["trades"][side] if t['symbol'] != stock['symbol'] or MomentumEngine.process_exit(t, ltp, stock, state, side)]

    @staticmethod
    def process_exit(trade, ltp, stock, state, side):
        trade['ltp'] = ltp
        is_exit = False
        
        if trade['side'] == 'BUY':
            if ltp >= trade['target_price'] or ltp <= trade['sl_price']: is_exit = True
        else:
            if ltp <= trade['target_price'] or ltp >= trade['sl_price']: is_exit = True
            
        if stock['symbol'] in state.get("manual_exits", set()): is_exit = True
        
        if is_exit:
            stock['status'] = 'CLOSED'
            state["manual_exits"].discard(stock['symbol'])
            logger.info(f"MOMENTUM TRADE CLOSED: {stock['symbol']} at {ltp}")
            return False # Remove from trades list
        return True # Keep in trades list