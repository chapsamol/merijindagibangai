import os
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, List, Tuple

import pytz
import redis.asyncio as redis

logger = logging.getLogger("Redis_Manager")
IST = pytz.timezone("Asia/Kolkata")

_r: Optional[redis.Redis] = None


def _redis_url() -> str:
    return (
        os.getenv("REDIS_TLS_URL")
        or os.getenv("REDIS_URL")
        or os.getenv("REDISCLOUD_URL")
        or ""
    )


def _seconds_until_ist_midnight() -> int:
    now = datetime.now(IST)
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return max(60, int((midnight - now).total_seconds()))


async def get_redis() -> redis.Redis:
    global _r
    if _r is not None:
        return _r

    url = _redis_url()
    if not url:
        raise RuntimeError("Redis URL not set. Set REDIS_TLS_URL or REDIS_URL in Heroku config vars.")

    kwargs = dict(
        decode_responses=True,
        socket_timeout=10,
        socket_connect_timeout=10,
        retry_on_timeout=True,
        health_check_interval=30,
    )

    if url.startswith("rediss://"):
        kwargs.update(ssl_cert_reqs=None)

    try:
        _r = redis.from_url(url, **kwargs)
        await _r.ping()
        logger.info("✅ Redis connected successfully.")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        _r = None
        raise

    return _r


class TradeControl:
    # -----------------------------
    # API KEY / SECRET
    # -----------------------------
    @staticmethod
    async def save_config(api_key: str, api_secret: str) -> bool:
        try:
            r = await get_redis()
            await r.set("nexus:config:api_key", str(api_key or ""))
            await r.set("nexus:config:api_secret", str(api_secret or ""))
            return True
        except Exception as e:
            logger.error(f"Failed to save api config: {e}")
            return False

    @staticmethod
    async def get_config() -> Tuple[str, str]:
        try:
            r = await get_redis()
            k = await r.get("nexus:config:api_key") or ""
            s = await r.get("nexus:config:api_secret") or ""
            return str(k), str(s)
        except Exception as e:
            logger.error(f"Failed to get api config: {e}")
            return "", ""

    # -----------------------------
    # ACCESS TOKEN
    # -----------------------------
    @staticmethod
    async def save_access_token(token: str) -> bool:
        try:
            r = await get_redis()
            await r.set("nexus:auth:access_token", str(token or ""))
            await r.set("nexus:auth:updated_at", datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"))
            return True
        except Exception as e:
            logger.error(f"Failed to save access token: {e}")
            return False

    @staticmethod
    async def get_access_token() -> str:
        try:
            r = await get_redis()
            return str(await r.get("nexus:auth:access_token") or "")
        except Exception as e:
            logger.error(f"Failed to get access token: {e}")
            return ""

    # -----------------------------
    # MARKET CACHE
    # -----------------------------
    @staticmethod
    async def save_market_data(token: str, market_data: dict) -> bool:
        try:
            r = await get_redis()
            key = f"nexus:market:{token}"
            await r.set(key, json.dumps(market_data))
            return True
        except Exception as e:
            logger.error(f"Failed to save market data {token}: {e}")
            return False

    @staticmethod
    async def get_market_data(token: str) -> dict:
        try:
            r = await get_redis()
            key = f"nexus:market:{token}"
            raw = await r.get(key)
            return json.loads(raw) if raw else {}
        except Exception as e:
            logger.error(f"Failed to get market data {token}: {e}")
            return {}

    @staticmethod
    async def delete_market_data(token: str) -> bool:
        try:
            r = await get_redis()
            key = f"nexus:market:{token}"
            await r.delete(key)
            return True
        except Exception as e:
            logger.error(f"Failed to delete market data {token}: {e}")
            return False

    @staticmethod
    async def get_all_market_data() -> Dict[str, dict]:
        try:
            r = await get_redis()
            out: Dict[str, dict] = {}
            async for key in r.scan_iter(match="nexus:market:*"):
                token = str(key).split(":")[-1]
                raw = await r.get(key)
                if raw:
                    try:
                        out[token] = json.loads(raw)
                    except Exception:
                        out[token] = {}
            return out
        except Exception as e:
            logger.error(f"Failed to get all market data: {e}")
            return {}

    @staticmethod
    async def set_last_sync() -> bool:
        try:
            r = await get_redis()
            await r.set("nexus:sync:last", datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"))
            return True
        except Exception as e:
            logger.error(f"Failed to set last sync: {e}")
            return False

    @staticmethod
    async def get_last_sync() -> str:
        try:
            r = await get_redis()
            return str(await r.get("nexus:sync:last") or "")
        except Exception as e:
            logger.error(f"Failed to get last sync: {e}")
            return ""

    # -----------------------------
    # STRATEGY SETTINGS
    # -----------------------------
    @staticmethod
    async def save_strategy_settings(side: str, cfg: dict) -> bool:
        try:
            r = await get_redis()
            key = f"nexus:settings:{side}"
            await r.set(key, json.dumps(cfg))
            return True
        except Exception as e:
            logger.error(f"Failed to save strategy settings {side}: {e}")
            return False

    @staticmethod
    async def get_strategy_settings(side: str) -> dict:
        try:
            r = await get_redis()
            key = f"nexus:settings:{side}"
            val = await r.get(key)
            return json.loads(val) if val else {}
        except Exception as e:
            logger.error(f"Failed to get strategy settings {side}: {e}")
            return {}

    # -----------------------------
    # SUBSCRIBE UNIVERSE
    # -----------------------------
    @staticmethod
    async def save_subscribe_universe(tokens: List[int], symbols: Optional[List[str]] = None) -> bool:
        try:
            r = await get_redis()
            await r.set("nexus:universe:tokens", json.dumps([int(x) for x in tokens]))
            if symbols is not None:
                await r.set("nexus:universe:symbols", json.dumps(list(symbols)))
            await r.set("nexus:universe:updated_at", datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"))
            return True
        except Exception as e:
            logger.error(f"Failed to save subscribe universe: {e}")
            return False

    @staticmethod
    async def get_subscribe_universe_tokens() -> List[int]:
        try:
            r = await get_redis()
            raw = await r.get("nexus:universe:tokens")
            if not raw:
                return []
            data = json.loads(raw)
            return [int(x) for x in data]
        except Exception as e:
            logger.error(f"Failed to get subscribe universe tokens: {e}")
            return []

    # -----------------------------
    # ATOMIC SIDE LIMIT RESERVATION (prevents "consume on failure")
    # -----------------------------
    _LUA_RESERVE_SIDE = r"""
    local key = KEYS[1]
    local limit = tonumber(ARGV[1])
    local ttl = tonumber(ARGV[2])

    local cur = tonumber(redis.call('GET', key) or '0')
    if cur >= limit then
      return 0
    end

    local newv = redis.call('INCR', key)
    if newv == 1 then
      redis.call('EXPIRE', key, ttl)
    end

    if newv > limit then
      redis.call('DECR', key)
      return 0
    end

    return 1
    """

    @staticmethod
    async def reserve_side_trade(side: str, limit: int) -> bool:
        """
        Reserves 1 slot for this side for today (IST). If order fails, call rollback_side_trade().
        """
        try:
            r = await get_redis()
            ttl = _seconds_until_ist_midnight()
            key = f"nexus:trades:side:{datetime.now(IST).strftime('%Y%m%d')}:{side}"
            ok = await r.eval(TradeControl._LUA_RESERVE_SIDE, 1, key, int(limit), int(ttl))
            return bool(ok)
        except Exception as e:
            logger.error(f"reserve_side_trade failed for {side}: {e}")
            return False

    @staticmethod
    async def rollback_side_trade(side: str) -> bool:
        try:
            r = await get_redis()
            key = f"nexus:trades:side:{datetime.now(IST).strftime('%Y%m%d')}:{side}"
            val = await r.get(key)
            if val is None:
                return True
            cur = int(val or 0)
            if cur > 0:
                await r.decr(key)
            return True
        except Exception as e:
            logger.error(f"rollback_side_trade failed for {side}: {e}")
            return False

    # -----------------------------
    # ATOMIC PER-SYMBOL LIMIT + OPEN LOCK (2 trades/symbol/day, 2nd only after close)
    # -----------------------------
    _LUA_RESERVE_SYMBOL = r"""
    local count_key = KEYS[1]
    local open_key  = KEYS[2]

    local max_trades = tonumber(ARGV[1])
    local ttl = tonumber(ARGV[2])

    if redis.call('EXISTS', open_key) == 1 then
      return {0, 'OPEN'}
    end

    local newv = redis.call('INCR', count_key)
    if newv == 1 then
      redis.call('EXPIRE', count_key, ttl)
    end

    if newv > max_trades then
      redis.call('DECR', count_key)
      return {0, 'MAX'}
    end

    local ok = redis.call('SET', open_key, '1', 'NX', 'EX', ttl)
    if not ok then
      redis.call('DECR', count_key)
      return {0, 'OPEN'}
    end

    return {1, tostring(newv)}
    """

    @staticmethod
    async def reserve_symbol_trade(symbol: str, max_trades: int = 2) -> Tuple[bool, str]:
        """
        Atomically:
          - block if symbol already has open lock
          - increment today's trade count for symbol (cap max_trades)
          - set open lock (expires at IST midnight for safety)
        If order fails, call rollback_symbol_trade().
        """
        try:
            r = await get_redis()
            day = datetime.now(IST).strftime("%Y%m%d")
            ttl = _seconds_until_ist_midnight()
            count_key = f"nexus:trades:symbol:{day}:{symbol}"
            open_key = f"nexus:pos:open:{day}:{symbol}"

            res = await r.eval(TradeControl._LUA_RESERVE_SYMBOL, 2, count_key, open_key, int(max_trades), int(ttl))
            # res is array [0/1, reason_or_count]
            ok = bool(int(res[0]))
            msg = str(res[1])
            return ok, msg
        except Exception as e:
            logger.error(f"reserve_symbol_trade failed for {symbol}: {e}")
            return False, "ERR"

    @staticmethod
    async def rollback_symbol_trade(symbol: str) -> bool:
        """
        If order placement fails AFTER reserve_symbol_trade(), rollback:
          - delete open lock
          - decrement count (if >0)
        """
        try:
            r = await get_redis()
            day = datetime.now(IST).strftime("%Y%m%d")
            count_key = f"nexus:trades:symbol:{day}:{symbol}"
            open_key = f"nexus:pos:open:{day}:{symbol}"
            pipe = r.pipeline()
            pipe.delete(open_key)
            pipe.get(count_key)
            out = await pipe.execute()
            val = out[1]
            if val is not None and int(val or 0) > 0:
                await r.decr(count_key)
            return True
        except Exception as e:
            logger.error(f"rollback_symbol_trade failed for {symbol}: {e}")
            return False

    @staticmethod
    async def release_symbol_lock(symbol: str) -> bool:
        """
        Call on successful close to allow 2nd trade (if count < max_trades).
        """
        try:
            r = await get_redis()
            day = datetime.now(IST).strftime("%Y%m%d")
            open_key = f"nexus:pos:open:{day}:{symbol}"
            await r.delete(open_key)
            return True
        except Exception as e:
            logger.error(f"release_symbol_lock failed for {symbol}: {e}")
            return False

    @staticmethod
    async def get_symbol_trade_count(symbol: str) -> int:
        try:
            r = await get_redis()
            day = datetime.now(IST).strftime("%Y%m%d")
            count_key = f"nexus:trades:symbol:{day}:{symbol}"
            v = await r.get(count_key)
            return int(v or 0)
        except Exception:
            return 0

    # -----------------------------
    # LEGACY METHODS (kept for compatibility, but engines now use reserve_* above)
    # -----------------------------
    @staticmethod
    async def can_trade(side: str, limit: int) -> bool:
        """
        Legacy: per-side counter without rollback on failure.
        Keep for backward compatibility; prefer reserve_side_trade().
        """
        try:
            r = await get_redis()
            key = f"nexus:trades:{side}"
            cur = await r.get(key)
            cur_i = int(cur) if cur else 0
            if cur_i >= int(limit):
                return False
            await r.incr(key)
            return True
        except Exception as e:
            logger.error(f"Failed can_trade check for {side}: {e}")
            return False

    @staticmethod
    async def reset_trade_counts() -> bool:
        try:
            r = await get_redis()
            for side in ["bull", "bear", "mom_bull", "mom_bear"]:
                await r.delete(f"nexus:trades:{side}")
            return True
        except Exception as e:
            logger.error(f"Failed reset_trade_counts: {e}")
            return False
