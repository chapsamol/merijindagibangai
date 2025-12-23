# # redis_manager.py
# """
# Nexus Redis Manager (Async)

# ✅ Supports:
# - API config (api_key/api_secret)
# - Zerodha access_token
# - Market cache (token -> {symbol,sma,pdh,pdl,prev_close,sync_time})
# - Strategy settings persistence (nexus:settings:{side})
# - Trade limit counters (per engine side)
# - Subscribe universe persistence (eligible tokens list)

# Environment:
# - On Heroku, set REDIS_URL (recommended) or REDIS_HOST/REDIS_PORT/REDIS_PASSWORD

# Dependencies:
# - redis>=4.2.0  (uses redis.asyncio)
# """

# import os
# import json
# import logging
# from datetime import datetime
# from typing import Any, Dict, Tuple, Optional, List

# import pytz
# import redis.asyncio as redis

# # -----------------------------
# # LOGGING / TIMEZONE
# # -----------------------------
# logger = logging.getLogger("Redis_Manager")
# IST = pytz.timezone("Asia/Kolkata")

# # -----------------------------
# # REDIS CONNECTION
# # -----------------------------
# REDIS_URL = os.getenv("REDIS_URL")

# REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
# REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
# REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)
# REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# # Key prefixes
# KEY_API_CONFIG = "nexus:config:api"              # {api_key, api_secret}
# KEY_ACCESS_TOKEN = "nexus:auth:access_token"     # string
# KEY_LAST_SYNC = "nexus:sync:last"                # string timestamp

# MARKET_KEY_PREFIX = "nexus:market:"              # nexus:market:{token}
# SETTINGS_KEY_PREFIX = "nexus:settings:"          # nexus:settings:{side}

# KEY_UNIVERSE_TOKENS = "nexus:universe:tokens"    # json list[int]
# KEY_UNIVERSE_SYMBOLS = "nexus:universe:symbols"  # json list[str]
# KEY_UNIVERSE_UPDATED = "nexus:universe:updated_at"

# TRADE_COUNT_PREFIX = "nexus:tradecount:"         # nexus:tradecount:{side} -> int daily
# TRADE_COUNT_DATE = "nexus:tradecount:date"       # YYYY-MM-DD

# # Create one global redis client
# if REDIS_URL:
#     r = redis.from_url(REDIS_URL, decode_responses=True)
# else:
#     r = redis.Redis(
#         host=REDIS_HOST,
#         port=REDIS_PORT,
#         password=REDIS_PASSWORD,
#         db=REDIS_DB,
#         decode_responses=True,
#     )


# class TradeControl:
#     # -----------------------------
#     # API CONFIG / AUTH
#     # -----------------------------
#     @staticmethod
#     async def save_config(api_key: str, api_secret: str) -> bool:
#         try:
#             await r.set(KEY_API_CONFIG, json.dumps({"api_key": api_key, "api_secret": api_secret}))
#             return True
#         except Exception as e:
#             logger.error(f"Failed to save api config: {e}")
#             return False

#     @staticmethod
#     async def get_config() -> Tuple[str, str]:
#         try:
#             raw = await r.get(KEY_API_CONFIG)
#             if not raw:
#                 return "", ""
#             data = json.loads(raw)
#             return (data.get("api_key", "") or "", data.get("api_secret", "") or "")
#         except Exception as e:
#             logger.error(f"Failed to get api config: {e}")
#             return "", ""

#     @staticmethod
#     async def save_access_token(access_token: str) -> bool:
#         try:
#             await r.set(KEY_ACCESS_TOKEN, access_token)
#             return True
#         except Exception as e:
#             logger.error(f"Failed to save access token: {e}")
#             return False

#     @staticmethod
#     async def get_access_token() -> str:
#         try:
#             return (await r.get(KEY_ACCESS_TOKEN)) or ""
#         except Exception as e:
#             logger.error(f"Failed to get access token: {e}")
#             return ""

#     # -----------------------------
#     # MARKET DATA CACHE
#     # -----------------------------
#     @staticmethod
#     async def save_market_data(token: str, market_data: Dict[str, Any]) -> bool:
#         """
#         Saves market cache per token.
#         Key: nexus:market:{token}
#         """
#         try:
#             key = f"{MARKET_KEY_PREFIX}{token}"
#             await r.set(key, json.dumps(market_data))
#             return True
#         except Exception as e:
#             logger.error(f"Failed to save market data {token}: {e}")
#             return False

#     @staticmethod
#     async def get_market_data(token: str) -> Dict[str, Any]:
#         try:
#             key = f"{MARKET_KEY_PREFIX}{token}"
#             raw = await r.get(key)
#             return json.loads(raw) if raw else {}
#         except Exception as e:
#             logger.error(f"Failed to get market data {token}: {e}")
#             return {}

#     @staticmethod
#     async def get_all_market_data() -> Dict[str, Dict[str, Any]]:
#         """
#         Returns dict token_str -> market_data
#         """
#         try:
#             pattern = f"{MARKET_KEY_PREFIX}*"
#             keys = await r.keys(pattern)
#             if not keys:
#                 return {}
#             vals = await r.mget(keys)

#             out: Dict[str, Dict[str, Any]] = {}
#             for k, v in zip(keys, vals):
#                 if not v:
#                     continue
#                 token = k.replace(MARKET_KEY_PREFIX, "")
#                 try:
#                     out[token] = json.loads(v)
#                 except Exception:
#                     continue
#             return out
#         except Exception as e:
#             logger.error(f"Failed to get all market data: {e}")
#             return {}

#     @staticmethod
#     async def delete_market_data(token: str) -> bool:
#         """
#         Deletes one instrument market cache entry: nexus:market:{token}
#         (useful when it fails SMA filter so stale data doesn't remain)
#         """
#         try:
#             key = f"{MARKET_KEY_PREFIX}{token}"
#             await r.delete(key)
#             return True
#         except Exception as e:
#             logger.error(f"Failed to delete market data {token}: {e}")
#             return False

#     @staticmethod
#     async def set_last_sync() -> bool:
#         try:
#             await r.set(KEY_LAST_SYNC, datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"))
#             return True
#         except Exception as e:
#             logger.error(f"Failed to set last sync: {e}")
#             return False

#     @staticmethod
#     async def get_last_sync() -> str:
#         try:
#             return (await r.get(KEY_LAST_SYNC)) or ""
#         except Exception as e:
#             logger.error(f"Failed to get last sync: {e}")
#             return ""

#     # -----------------------------
#     # STRATEGY SETTINGS (PERSISTED)
#     # -----------------------------
#     @staticmethod
#     async def save_strategy_settings(side: str, cfg: dict) -> bool:
#         try:
#             key = f"{SETTINGS_KEY_PREFIX}{side}"
#             await r.set(key, json.dumps(cfg))
#             return True
#         except Exception as e:
#             logger.error(f"Failed to save strategy settings {side}: {e}")
#             return False

#     @staticmethod
#     async def get_strategy_settings(side: str) -> dict:
#         try:
#             key = f"{SETTINGS_KEY_PREFIX}{side}"
#             val = await r.get(key)
#             return json.loads(val) if val else {}
#         except Exception as e:
#             logger.error(f"Failed to get strategy settings {side}: {e}")
#             return {}

#     # -----------------------------
#     # SUBSCRIBE UNIVERSE (ELIGIBLE TOKENS)
#     # -----------------------------
#     @staticmethod
#     async def save_subscribe_universe(tokens: List[int], symbols: Optional[List[str]] = None) -> bool:
#         """
#         Store eligible instrument tokens to subscribe all day.
#         """
#         try:
#             await r.set(KEY_UNIVERSE_TOKENS, json.dumps([int(x) for x in tokens]))
#             if symbols is not None:
#                 await r.set(KEY_UNIVERSE_SYMBOLS, json.dumps(list(symbols)))
#             await r.set(KEY_UNIVERSE_UPDATED, datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"))
#             return True
#         except Exception as e:
#             logger.error(f"Failed to save subscribe universe: {e}")
#             return False

#     @staticmethod
#     async def get_subscribe_universe_tokens() -> List[int]:
#         """
#         Read eligible instrument tokens list.
#         """
#         try:
#             raw = await r.get(KEY_UNIVERSE_TOKENS)
#             if not raw:
#                 return []
#             data = json.loads(raw)
#             return [int(x) for x in data]
#         except Exception as e:
#             logger.error(f"Failed to get subscribe universe tokens: {e}")
#             return []

#     @staticmethod
#     async def get_subscribe_universe_symbols() -> List[str]:
#         try:
#             raw = await r.get(KEY_UNIVERSE_SYMBOLS)
#             if not raw:
#                 return []
#             data = json.loads(raw)
#             return [str(x) for x in data]
#         except Exception as e:
#             logger.error(f"Failed to get subscribe universe symbols: {e}")
#             return []

#     @staticmethod
#     async def get_subscribe_universe_updated_at() -> str:
#         try:
#             return (await r.get(KEY_UNIVERSE_UPDATED)) or ""
#         except Exception as e:
#             logger.error(f"Failed to get subscribe universe updated_at: {e}")
#             return ""

#     # -----------------------------
#     # DAILY TRADE LIMIT COUNTERS
#     # -----------------------------
#     @staticmethod
#     async def _roll_trade_count_if_new_day() -> None:
#         """
#         Ensure trade counters reset once per day (IST).
#         """
#         today = datetime.now(IST).strftime("%Y-%m-%d")
#         try:
#             stored = await r.get(TRADE_COUNT_DATE)
#             if stored != today:
#                 # New day: delete all counters
#                 keys = await r.keys(f"{TRADE_COUNT_PREFIX}*")
#                 if keys:
#                     await r.delete(*keys)
#                 await r.set(TRADE_COUNT_DATE, today)
#         except Exception as e:
#             logger.error(f"Trade count roll error: {e}")

#     @staticmethod
#     async def can_trade(side: str, max_trades: int) -> bool:
#         """
#         Returns True if side has remaining trades for the day.
#         NOTE: This function is used by engines before placing orders.
#         """
#         try:
#             await TradeControl._roll_trade_count_if_new_day()
#             key = f"{TRADE_COUNT_PREFIX}{side}"
#             current = await r.get(key)
#             current_n = int(current) if current else 0
#             if current_n >= int(max_trades):
#                 return False
#             # increment and allow
#             await r.incr(key)
#             return True
#         except Exception as e:
#             logger.error(f"can_trade failed for {side}: {e}")
#             # safest: block trade if counter broken
#             return False

#     # -----------------------------
#     # UTIL
#     # -----------------------------
#     @staticmethod
#     async def ping() -> bool:
#         try:
#             return bool(await r.ping())
#         except Exception:
#             return False
# redis_manager.py
import os
import json
import ssl
import logging
from datetime import datetime
from typing import Any, Dict, Optional, List, Tuple

import pytz
import redis.asyncio as redis

logger = logging.getLogger("Redis_Manager")
IST = pytz.timezone("Asia/Kolkata")

# Global singleton client (lazy init)
_r: Optional[redis.Redis] = None


def _redis_url() -> str:
    """
    Prefer TLS url on Heroku. Heroku provides:
      - REDIS_TLS_URL (rediss://...)
      - REDIS_URL (redis://...)
    """
    return (
        os.getenv("REDIS_TLS_URL")
        or os.getenv("REDIS_URL")
        or os.getenv("REDISCLOUD_URL")
        or ""
    )


async def get_redis() -> redis.Redis:
    """
    Lazy init Redis client with Heroku-friendly TLS settings.
    
    Fix for: AbstractConnection.__init__() got an unexpected keyword argument 'ssl'
    - When using from_url with a 'rediss://' scheme, redis-py already knows to use SSL.
    - Adding 'ssl=True' manually causes a conflict in the AbstractConnection constructor.
    - We only need to provide 'ssl_cert_reqs' as None for Heroku's self-signed certs.
    """
    global _r
    if _r is not None:
        return _r

    url = _redis_url()
    if not url:
        raise RuntimeError("Redis URL not set. Set REDIS_TLS_URL or REDIS_URL in Heroku config vars.")

    # Base Redis client kwargs
    kwargs = dict(
        decode_responses=True,             # store strings (JSON) cleanly
        socket_timeout=10,
        socket_connect_timeout=10,
        retry_on_timeout=True,
        health_check_interval=30,
    )

    # TLS handling for rediss://
    if url.startswith("rediss://"):
        # ✅ FIX: Do NOT pass 'ssl=True' here when using from_url.
        # The 'rediss' prefix in the URL already triggers SSL logic.
        # We only pass the certificate requirement bypass for Heroku.
        kwargs.update(
            ssl_cert_reqs=None, 
        )

    try:
        # Use from_url which parses the scheme and applies SSL logic internally
        _r = redis.from_url(url, **kwargs)
        await _r.ping()
        logger.info("✅ Redis connected successfully.")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        _r = None # Reset so next call retries
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
    # MARKET CACHE (SMA/PDH/PDL/PREV_CLOSE)
    # key: nexus:market:{token}
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
        """
        Deletes one instrument market cache entry: nexus:market:{token}
        """
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
        Returns dict: { token_str: {...market_data...}, ... }
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
    # TRADE LIMIT
    # -----------------------------
    @staticmethod
    async def can_trade(side: str, limit: int) -> bool:
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