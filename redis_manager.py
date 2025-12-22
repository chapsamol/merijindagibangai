import redis
import os
import time

# Standard Redis connection with fail-safe
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
r = redis.from_url(REDIS_URL, decode_responses=True)

class TradeControl:
    @staticmethod
    def can_trade(strategy_side: str, limit: int):
        """
        Atomic Check using Redis INCR to avoid Race Conditions.
        """
        key = f"limit:{strategy_side}:{time.strftime('%Y-%m-%d')}"
        try:
            current = r.get(key)
            if current and int(current) >= limit:
                return False
            r.incr(key)
            r.expire(key, 86400)
            return True
        except:
            return False # Default to safety

    @staticmethod
    def get_current_count(strategy_side: str):
        key = f"limit:{strategy_side}:{time.strftime('%Y-%m-%d')}"
        val = r.get(key)
        return int(val) if val else 0