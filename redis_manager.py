# -*- coding: utf-8 -*-
"""
Nexus Redis Manager (Dhan-ready, architecture unchanged)

✅ Works with redis.asyncio (redis-py) correctly
✅ Atomic Lua-based counters:
   - per-side daily trade limit
   - per-symbol daily max trades
   - open-position lock per symbol
✅ Daily keys (IST) so limits reset automatically each day
✅ Compatible with your app (no breaking changes)

IMPORTANT DHAN NOTE:
- In Dhan version of app, "token" everywhere means DHAN security_id.
  Example:
    nexus:market:{token}  => nexus:market:{security_id}

Frontend mapping (kept as-is to avoid UI changes):
- save_config(api_key, api_secret) stores:
    api_key    -> DHAN_CLIENT_ID
    api_secret -> DHAN_ACCESS_TOKEN
"""

import os
import json
import logging
import asyncio
from datetime import datetime
from typing import Dict, Optional, List, Tuple

import pytz
import redis.asyncio as redis

logger = logging.getLogger("Redis_Manager")
IST = pytz.timezone("Asia/Kolkata")

_r: Optional[redis.Redis] = None
_r_init_lock = asyncio.Lock()


# -----------------------------
# Helpers
# -----------------------------
def _redis_url() -> str:
    return (
        os.getenv("REDIS_TLS_URL")
        or os.getenv("REDIS_URL")
        or os.getenv("REDISCLOUD_URL")
        or ""
    )


def _ist_day_key() -> str:
    return datetime.now(IST).strftime("%Y%m%d")


def _seconds_until_ist_eod() -> int:
    now = datetime.now(IST)
    eod = now.replace(hour=23, minute=59, second=59, microsecond=0)
    return max(60, int((eod - now).total_seconds()))


async def get_redis() -> redis.Redis:
    """
    Singleton async Redis client with safe concurrent init.
    """
    global _r
    if _r is not None:
        return _r

    async with _r_init_lock:
        if _r is not None:
            return _r

        url = _redis_url()
        if not url:
            raise RuntimeError("Redis URL not set. Set REDIS_TLS_URL or REDIS_URL.")

        kwargs = dict(
            decode_responses=True,
            socket_timeout=10,
            socket_connect_timeout=10,
            retry_on_timeout=True,
            health_check_interval=30,
        )

        # TLS redis
        if url.startswith("rediss://"):
            # redis-py accepts ssl_cert_reqs=None for permissive TLS validation
            kwargs.update(ssl_cert_reqs=None)

        client = redis.from_url(url, **kwargs)
        await client.ping()

        _r = client
        logger.info("✅ Redis connected successfully.")
        return _r


# -----------------------------
# LUA scripts
# -----------------------------
_LUA_RESERVE_SIDE = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local cur = tonumber(redis.call('GET', key) or '0')
if cur >= limit then
  return 0
end
redis.call('INCR', key)
return 1
"""

_LUA_ROLLBACK_SIDE = """
local key = KEYS[1]
local cur = tonumber(redis.call('GET', key) or '0')
if cur <= 0 then
  redis.call('SET', key, 0)
  return 1
end
redis.call('DECR', key)
return 1
"""

_LUA_RESERVE_SYMBOL = """
local count_key = KEYS[1]
local lock_key  = KEYS[2]

local max_trades = tonumber(ARGV[1])
local lock_ttl   = tonumber(ARGV[2])

if redis.call('EXISTS', lock_key) == 1 then
  return {0, "LOCKED"}
end

local cur = tonumber(redis.call('GET', count_key) or '0')
if cur >= max_trades then
  return {0, "MAX_TRADES"}
end

redis.call('INCR', count_key)

local ok = redis.call('SET', lock_key, '1', 'NX', 'EX', lock_ttl)
if not ok then
  redis.call('DECR', count_key)
  return {0, "LOCKED"}
end

return {1, "OK"}
"""

_LUA_ROLLBACK_SYMBOL = """
local count_key = KEYS[1]
local lock_key  = KEYS[2]

redis.call('DEL', lock_key)

local cur = tonumber(redis.call('GET', count_key) or '0')
if cur <= 0 then
  redis.call('SET', count_key, 0)
else
  redis.call('DECR', count_key)
end
return 1
"""


class TradeControl:
    # -----------------------------
    # CONFIG (kept for dashboard compatibility)
    # api_key    -> DHAN_CLIENT_ID
    # api_secret -> DHAN_ACCESS_TOKEN
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

    # Optional aliases (no UI change needed)
    @staticmethod
    async def save_dhan_config(client_id: str, access_token: str) -> bool:
        return await TradeControl.save_config(client_id, access_token)

    @staticmethod
    async def get_dhan_config() -> Tuple[str, str]:
        return await TradeControl.get_config()

    # -----------------------------
    # LEGACY ACCESS TOKEN (Kite-era)
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
    # key: nexus:market:{token}
    # DHAN: token == security_id
    # stores: symbol, sma, pdh, pdl, prev_close, etc.
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
        """
        Returns dict keyed by token/security_id (string form).
        """
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

    # -----------------------------
    # LAST SYNC
    # -----------------------------
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
    # DHAN: tokens are security_ids
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
    # ATOMIC SIDE TRADE LIMITS (daily)
    # NOTE: This is a DAILY counter, not "open positions".
    # So: reserve on entry; rollback only on entry failure.
    # -----------------------------
    @staticmethod
    async def reserve_side_trade(side: str, limit: int) -> bool:
        try:
            r = await get_redis()
            day = _ist_day_key()
            key = f"nexus:trades:side:{day}:{side}"
            ok = await r.eval(_LUA_RESERVE_SIDE, 1, key, str(int(limit)))
            await r.expire(key, _seconds_until_ist_eod())
            return int(ok) == 1
        except Exception as e:
            logger.error(f"reserve_side_trade failed for {side}: {e}")
            return False

    @staticmethod
    async def rollback_side_trade(side: str) -> bool:
        try:
            r = await get_redis()
            day = _ist_day_key()
            key = f"nexus:trades:side:{day}:{side}"
            await r.eval(_LUA_ROLLBACK_SIDE, 1, key)
            await r.expire(key, _seconds_until_ist_eod())
            return True
        except Exception as e:
            logger.error(f"rollback_side_trade failed for {side}: {e}")
            return False

    @staticmethod
    async def reset_trade_counts() -> bool:
        try:
            r = await get_redis()
            day = _ist_day_key()
            for side in ["bull", "bear", "mom_bull", "mom_bear"]:
                await r.delete(f"nexus:trades:side:{day}:{side}")
            return True
        except Exception as e:
            logger.error(f"Failed reset_trade_counts: {e}")
            return False

    # -----------------------------
    # ATOMIC PER-SYMBOL LIMIT (daily count) + OPEN LOCK (single open position)
    # reserve_symbol_trade():
    #   - increments DAILY count (count_key)
    #   - acquires OPEN lock (lock_key)
    # On normal close: call release_symbol_lock() ONLY (do not decrement daily count)
    # On entry failure: call rollback_symbol_trade() to undo both
    # -----------------------------
    @staticmethod
    async def reserve_symbol_trade(
        symbol: str,
        max_trades: int = 2,
        lock_ttl_sec: int = 1800
    ) -> Tuple[bool, str]:
        symbol = str(symbol or "").strip().upper()
        if not symbol:
            return False, "ERROR"

        try:
            r = await get_redis()
            day = _ist_day_key()
            count_key = f"nexus:trades:symbol:{day}:{symbol}"
            lock_key = f"nexus:pos:open:{symbol}"

            res = await r.eval(
                _LUA_RESERVE_SYMBOL,
                2,
                count_key,
                lock_key,
                str(int(max_trades)),
                str(int(lock_ttl_sec)),
            )

            ok = int(res[0]) == 1
            reason = str(res[1])
            await r.expire(count_key, _seconds_until_ist_eod())
            return ok, reason

        except Exception as e:
            logger.error(f"reserve_symbol_trade failed for {symbol}: {e}")
            return False, "ERROR"

    @staticmethod
    async def rollback_symbol_trade(symbol: str) -> bool:
        symbol = str(symbol or "").strip().upper()
        if not symbol:
            return False

        try:
            r = await get_redis()
            day = _ist_day_key()
            count_key = f"nexus:trades:symbol:{day}:{symbol}"
            lock_key = f"nexus:pos:open:{symbol}"
            await r.eval(_LUA_ROLLBACK_SYMBOL, 2, count_key, lock_key)
            await r.expire(count_key, _seconds_until_ist_eod())
            return True
        except Exception as e:
            logger.error(f"rollback_symbol_trade failed for {symbol}: {e}")
            return False

    @staticmethod
    async def release_symbol_lock(symbol: str) -> bool:
        symbol = str(symbol or "").strip().upper()
        if not symbol:
            return False
        try:
            r = await get_redis()
            await r.delete(f"nexus:pos:open:{symbol}")
            return True
        except Exception as e:
            logger.error(f"release_symbol_lock failed for {symbol}: {e}")
            return False

    @staticmethod
    async def get_symbol_trade_count(symbol: str) -> int:
        symbol = str(symbol or "").strip().upper()
        if not symbol:
            return 0
        try:
            r = await get_redis()
            day = _ist_day_key()
            v = await r.get(f"nexus:trades:symbol:{day}:{symbol}")
            return int(v) if v else 0
        except Exception as e:
            logger.error(f"get_symbol_trade_count failed for {symbol}: {e}")
            return 0

    # -----------------------------
    # LEGACY: can_trade (kept)
    # -----------------------------
    @staticmethod
    async def can_trade(side: str, limit: int) -> bool:
        return await TradeControl.reserve_side_trade(side, limit)
