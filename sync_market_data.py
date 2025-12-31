"""
Nexus Async Market Data Sync (Dhan)

Fixes included:
- Removes datetime.utcfromtimestamp() DeprecationWarning (Py3.12+)
- Live progress logging while syncing (so it never looks "stuck")
- Robust retry + exponential backoff on HTTP 429 / 5xx / transient failures
- Global rate-limiter so concurrency doesn't instantly trigger 429
- Safer persistence: by default DOES NOT delete old Redis market data on API errors
- Reads access token from BOTH places:
    1) nexus:auth:access_token (TradeControl.get_access_token)
    2) nexus:config:api_secret (TradeControl.get_config fallback)

NEW FIXES (2025-12-31):
- Works with updated redis_manager TLS logic (secure by default, optional insecure)
- Adds client-id header for Dhan (many endpoints require it)
- Instrument list downloader supports BOTH:
    - CSV scrip master
    - JSON /v2/instruments endpoints (previous version assumed CSV only)
- More defensive parsing of historical response (supports nested payloads)
"""

import asyncio
import os
import logging
import json
import csv
import io
import random
from datetime import datetime, timedelta, date, timezone
from typing import Dict, List, Tuple, Optional, Any
from urllib import request, error

import pytz
from redis_manager import TradeControl

# -----------------------------
# LOGGING
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ASYNC-SYNC-DHAN] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("Sync_Command_Dhan")
IST = pytz.timezone("Asia/Kolkata")

# -----------------------------
# FILTERS / TUNING
# -----------------------------
MIN_VOL_SMA = int(os.getenv("MIN_VOL_SMA", "1000"))

MAX_CONCURRENCY = int(os.getenv("SYNC_CONCURRENCY", "3"))
MIN_REQ_INTERVAL_SEC = float(os.getenv("SYNC_MIN_REQ_INTERVAL_SEC", "0.60"))
REQ_SLEEP = float(os.getenv("SYNC_SLEEP_SEC", "0.05"))

HIST_LOOKBACK_DAYS = int(os.getenv("HIST_LOOKBACK_DAYS", "20"))  # keep >= 10
EXCHANGE_SEGMENT = os.getenv("DHAN_EXCHANGE_SEGMENT", "NSE_EQ")
INSTRUMENT = os.getenv("DHAN_INSTRUMENT", "EQUITY")

PROGRESS_EVERY = int(os.getenv("SYNC_PROGRESS_EVERY", "25"))
SYNC_LIMIT_SYMBOLS = int(os.getenv("SYNC_LIMIT_SYMBOLS", "0"))

KEEP_OLD_ON_ERROR = os.getenv("SYNC_KEEP_OLD_ON_ERROR", "1") == "1"

MAX_RETRIES = int(os.getenv("SYNC_MAX_RETRIES", "6"))
BACKOFF_BASE = float(os.getenv("SYNC_BACKOFF_BASE", "0.8"))
BACKOFF_MAX = float(os.getenv("SYNC_BACKOFF_MAX", "20.0"))

# Instrument list endpoints (try in order)
DHAN_INSTRUMENT_URLS = [
    os.getenv("DHAN_INSTRUMENTS_URL", "").strip(),
    "https://images.dhan.co/api-data/api-scrip-master.csv",
    f"https://api.dhan.co/v2/instruments/{EXCHANGE_SEGMENT}",
    f"https://api.dhan.co/v2/instrument/{EXCHANGE_SEGMENT}",
]
DHAN_INSTRUMENT_URLS = [u for u in DHAN_INSTRUMENT_URLS if u]

DHAN_BASE = os.getenv("DHAN_BASE_URL", "https://api.dhan.co").rstrip("/")

# -----------------------------
# STOCK UNIVERSE (your mapping)
# -----------------------------
# !!! KEEP YOUR FULL EXISTING DICT HERE (exactly as-is) !!!
STOCK_INDEX_MAPPING = {
    # paste your full mapping here (your current dict is fine)
    # ...
}

# -----------------------------
# Small utils
# -----------------------------
def _now_ist() -> datetime:
    return datetime.now(IST)

def _safe_float(x, d=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d

def _safe_int(x, d=0) -> int:
    try:
        return int(float(x))  # sometimes "123.0"
    except Exception:
        return d

def _pick_field(fieldnames: List[str], candidates: List[str]) -> Optional[str]:
    lower_map = {f.lower(): f for f in fieldnames}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None

def _env_flag(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in ("1", "true", "yes", "y", "on")

def _build_headers(client_id: str, access_token: str, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    Dhan often expects BOTH:
      - client-id
      - access-token
    """
    h = {
        "Accept": "application/json, text/csv;q=0.9, */*;q=0.8",
        "User-Agent": "Nexus-Sync/1.0",
    }
    if client_id:
        h["client-id"] = client_id
    if access_token:
        h["access-token"] = access_token
    if extra:
        h.update(extra)
    return h

def _unwrap_payload(obj: Any) -> Any:
    """
    Some endpoints return {"data": ...} or {"payload": ...}.
    Try to unwrap common containers.
    """
    if isinstance(obj, dict):
        for k in ("data", "payload", "result"):
            if k in obj and obj[k] is not None:
                return obj[k]
    return obj

# -----------------------------
# Global async rate limiter
# -----------------------------
class _GlobalRateLimiter:
    def __init__(self, min_interval_sec: float):
        self.min_interval_sec = max(0.0, float(min_interval_sec))
        self._lock = asyncio.Lock()
        self._last_ts = 0.0  # monotonic

    async def wait(self):
        if self.min_interval_sec <= 0:
            return
        async with self._lock:
            now = asyncio.get_running_loop().time()
            elapsed = now - self._last_ts
            if elapsed < self.min_interval_sec:
                await asyncio.sleep(self.min_interval_sec - elapsed)
            self._last_ts = asyncio.get_running_loop().time()

_RATE = _GlobalRateLimiter(MIN_REQ_INTERVAL_SEC)

# -----------------------------
# HTTP helpers (urllib) + retry/backoff
# -----------------------------
def _http_get_bytes(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 60) -> bytes:
    hdrs = headers or {}
    req = request.Request(url, headers=hdrs, method="GET")
    with request.urlopen(req, timeout=timeout) as resp:
        return resp.read()

def _http_post_json_once(url: str, payload: dict, headers: Optional[Dict[str, str]] = None, timeout: int = 60) -> dict:
    body = json.dumps(payload).encode("utf-8")
    hdrs = {"Content-Type": "application/json", "Accept": "application/json"}
    if headers:
        hdrs.update(headers)
    req = request.Request(url, data=body, headers=hdrs, method="POST")
    with request.urlopen(req, timeout=timeout) as resp:
        data = resp.read().decode("utf-8", errors="ignore").strip()
        if not data:
            return {}
        try:
            return json.loads(data)
        except Exception:
            # Some gateways send non-json errors with 200 (rare)
            return {"_raw": data[:2000]}

def _extract_retry_after_seconds(err_obj: Exception) -> Optional[float]:
    try:
        if isinstance(err_obj, error.HTTPError):
            ra = err_obj.headers.get("Retry-After")
            if ra:
                return float(ra)
    except Exception:
        pass
    return None

async def _http_post_json_retry(url: str, payload: dict, headers: Optional[Dict[str, str]] = None, timeout: int = 60) -> dict:
    """
    Retries on 429/5xx/timeout/transient.
    Uses exponential backoff + jitter; respects Retry-After when available.
    """
    last_exc: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await _RATE.wait()
            return await asyncio.to_thread(_http_post_json_once, url, payload, headers, timeout)

        except Exception as e:
            last_exc = e
            status = None

            if isinstance(e, error.HTTPError):
                status = getattr(e, "code", None)

            retryable = False
            if status == 429:
                retryable = True
            elif status is not None and 500 <= int(status) <= 599:
                retryable = True
            elif isinstance(e, (error.URLError, TimeoutError)):
                retryable = True

            if not retryable or attempt >= MAX_RETRIES:
                raise

            retry_after = _extract_retry_after_seconds(e)
            if retry_after is not None and retry_after > 0:
                sleep_s = min(float(retry_after), BACKOFF_MAX)
            else:
                base = min(BACKOFF_BASE * (2 ** (attempt - 1)), BACKOFF_MAX)
                jitter = random.uniform(0.0, base * 0.25)
                sleep_s = min(BACKOFF_MAX, base + jitter)

            logger.warning(
                f"Retryable HTTP error (attempt {attempt}/{MAX_RETRIES}) -> sleep {sleep_s:.2f}s | "
                f"status={status} | err={e}"
            )
            await asyncio.sleep(sleep_s)

    if last_exc:
        raise last_exc
    return {}

# -----------------------------
# Instrument list download (CSV or JSON)
# -----------------------------
def _parse_instruments_csv(text: str, wanted: set) -> Dict[str, str]:
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise RuntimeError("Instrument CSV has no headers")

    sym_col = _pick_field(reader.fieldnames, [
        "tradingsymbol", "tradingSymbol", "TRADING_SYMBOL", "SYMBOL",
        "SEM_TRADING_SYMBOL", "SEM_CUSTOM_SYMBOL", "customSymbol"
    ])
    sec_col = _pick_field(reader.fieldnames, [
        "securityId", "SECURITY_ID", "SECURITYID",
        "SEM_SMST_SECURITY_ID", "SEM_SECURITY_ID"
    ])
    exch_col = _pick_field(reader.fieldnames, [
        "exchangeSegment", "EXCHANGE_SEGMENT", "SEM_EXM_EXCH_ID", "exchange"
    ])

    if not sym_col or not sec_col:
        raise RuntimeError(f"Cannot detect columns. headers={reader.fieldnames[:30]}")

    out: Dict[str, str] = {}
    for row in reader:
        sym = (row.get(sym_col) or "").strip().upper()
        if sym not in wanted:
            continue

        if exch_col:
            exv = (row.get(exch_col) or "").strip().upper()
            if exv and ("NSE" not in exv and "NSE_EQ" not in exv and exv != "NSE"):
                continue

        sid = (row.get(sec_col) or "").strip()
        if sid:
            out[sym] = sid

    return out

def _parse_instruments_json(obj: Any, wanted: set) -> Dict[str, str]:
    obj = _unwrap_payload(obj)

    # most common: list[dict]
    items: List[dict] = []
    if isinstance(obj, list):
        items = [x for x in obj if isinstance(x, dict)]
    elif isinstance(obj, dict):
        # maybe {"instruments": [...]}
        for k in ("instruments", "instrument", "records", "items"):
            v = obj.get(k)
            if isinstance(v, list):
                items = [x for x in v if isinstance(x, dict)]
                break

    if not items:
        raise RuntimeError("Instrument JSON has no list payload")

    out: Dict[str, str] = {}

    # Candidate keys
    sym_keys = ["tradingSymbol", "tradingsymbol", "TRADINGSYMBOL", "symbol", "SYMBOL", "customSymbol", "SEM_TRADING_SYMBOL"]
    sid_keys = ["securityId", "SECURITY_ID", "securityID", "SEM_SECURITY_ID", "SEM_SMST_SECURITY_ID"]
    exch_keys = ["exchangeSegment", "EXCHANGE_SEGMENT", "exchange", "SEM_EXM_EXCH_ID"]

    def pick(d: dict, keys: List[str]) -> str:
        for k in keys:
            if k in d and d[k] is not None:
                return str(d[k]).strip()
        # try case-insensitive
        lower = {str(k).lower(): k for k in d.keys()}
        for k in keys:
            lk = str(k).lower()
            if lk in lower:
                return str(d.get(lower[lk]) or "").strip()
        return ""

    for it in items:
        sym = pick(it, sym_keys).upper()
        if not sym or sym not in wanted:
            continue

        exv = pick(it, exch_keys).upper()
        if exv and ("NSE" not in exv and "NSE_EQ" not in exv and exv != "NSE"):
            continue

        sid = pick(it, sid_keys)
        if sid:
            out[sym] = sid

    return out

async def _download_symbol_to_security_id_map(client_id: str, access_token: str) -> Dict[str, str]:
    """
    Downloads Dhan instrument list (CSV or JSON) and returns {TRADINGSYMBOL: securityId}
    Keeps only symbols present in STOCK_INDEX_MAPPING.
    """
    wanted = set(k.upper() for k in STOCK_INDEX_MAPPING.keys())
    headers = _build_headers(client_id, access_token)

    last_err = None
    for url in DHAN_INSTRUMENT_URLS:
        if not url:
            continue
        try:
            logger.info(f"Downloading instrument list: {url}")
            raw = await asyncio.to_thread(_http_get_bytes, url, headers, 120)
            text = raw.decode("utf-8", errors="ignore").strip()

            # Decide CSV vs JSON
            if url.lower().endswith(".csv") or ("," in text.splitlines()[0] and "security" in text.lower()):
                out = _parse_instruments_csv(text, wanted)
            else:
                try:
                    obj = json.loads(text)
                except Exception:
                    # sometimes still CSV without .csv extension
                    out = _parse_instruments_csv(text, wanted)
                else:
                    out = _parse_instruments_json(obj, wanted)

            logger.info(f"Instrument map resolved for {len(out)} / {len(wanted)} symbols")
            return out

        except Exception as e:
            last_err = e
            logger.warning(f"Instrument list failed from {url}: {e}")

    raise RuntimeError(f"Instrument list download failed. Last error: {last_err}")

# -----------------------------
# SMA computation
# -----------------------------
def _compute_sma_from_records(records: List[dict]) -> float:
    """
    SMA definition:
      avg_vol_per_minute = (sum(last 5 completed sessions volume)) / 1875
    1875 = 5 days * 375 minutes/session
    """
    if not records:
        return 0.0

    today_ist = _now_ist().date()
    last_candle_date = records[-1]["date"]
    if isinstance(last_candle_date, datetime):
        last_candle_date = last_candle_date.date()

    if last_candle_date == today_ist:
        completed_records = records[-6:-1]
    else:
        completed_records = records[-5:]

    if len(completed_records) < 5:
        return 0.0

    total_vol = sum(_safe_int(day.get("volume", 0), 0) for day in completed_records)
    avg_vol_per_minute = total_vol / 1875.0
    return round(avg_vol_per_minute, 2)

def _dhan_daily_to_records(resp: dict) -> List[dict]:
    """
    Dhan daily historical typically returns arrays: open/high/low/close/volume/timestamp.
    Some variants wrap payload (data/result). We unwrap defensively.
    """
    if not isinstance(resp, dict):
        return []

    base = _unwrap_payload(resp)
    if isinstance(base, dict):
        resp = base

    ts = resp.get("timestamp") or resp.get("timestamps") or []
    o = resp.get("open") or []
    h = resp.get("high") or []
    l = resp.get("low") or []
    c = resp.get("close") or []
    v = resp.get("volume") or []

    n = min(len(ts), len(o), len(h), len(l), len(c), len(v))
    if n <= 0:
        return []

    out: List[dict] = []
    for i in range(n):
        dt_utc = datetime.fromtimestamp(int(ts[i]), tz=timezone.utc)
        dt_ist = dt_utc.astimezone(IST)
        out.append({
            "date": dt_ist,
            "open": _safe_float(o[i]),
            "high": _safe_float(h[i]),
            "low": _safe_float(l[i]),
            "close": _safe_float(c[i]),
            "volume": _safe_int(v[i]),
        })
    return out

# -----------------------------
# One symbol fetch
# -----------------------------
async def _fetch_one(
    client_id: str,
    access_token: str,
    security_id: str,
    symbol: str,
    sem: asyncio.Semaphore,
) -> Tuple[int, str, str, float, Optional[dict]]:
    """
    Fetch Dhan daily historical -> compute SMA -> build market_data.
    token stored in Redis = securityId (int)
    """
    async with sem:
        try:
            today = _now_ist().date()
            to_d: date = today + timedelta(days=1)  # toDate non-inclusive; +1 includes today if available
            from_d: date = to_d - timedelta(days=HIST_LOOKBACK_DAYS)

            payload = {
                "securityId": str(security_id),
                "exchangeSegment": EXCHANGE_SEGMENT,
                "instrument": INSTRUMENT,
                "expiryCode": 0,
                "oi": False,
                "fromDate": from_d.strftime("%Y-%m-%d"),
                "toDate": to_d.strftime("%Y-%m-%d"),
            }

            url = f"{DHAN_BASE}/v2/charts/historical"
            headers = _build_headers(client_id, access_token)

            resp = await _http_post_json_retry(url, payload, headers, 60)
            records = _dhan_daily_to_records(resp)

            if not records:
                return (int(security_id), symbol, "ERROR", 0.0, None)

            last_rec_date = records[-1]["date"].date()
            if last_rec_date == today and len(records) >= 2:
                last_completed = records[-2]
            else:
                last_completed = records[-1]

            sma = _compute_sma_from_records(records)
            if sma < MIN_VOL_SMA:
                return (int(security_id), symbol, "FILTERED", sma, None)

            market_data = {
                "symbol": symbol,
                "sma": float(sma),
                "pdh": _safe_float(last_completed.get("high", 0.0), 0.0),
                "pdl": _safe_float(last_completed.get("low", 0.0), 0.0),
                "prev_close": _safe_float(last_completed.get("close", 0.0), 0.0),
                "sync_time": _now_ist().strftime("%Y-%m-%d %H:%M:%S"),
            }

            return (int(security_id), symbol, "OK", sma, market_data)

        except Exception as e:
            logger.error(f"Error syncing {symbol} ({security_id}): {e}")
            return (int(security_id), symbol, "ERROR", 0.0, None)

        finally:
            if REQ_SLEEP > 0:
                await asyncio.sleep(REQ_SLEEP)

# -----------------------------
# Persist results
# -----------------------------
async def _persist_results(results: List[Tuple[int, str, str, float, Optional[dict]]]) -> Tuple[List[int], List[str]]:
    eligible_tokens: List[int] = []
    eligible_symbols: List[str] = []

    async def _save_ok(t_id: int, md: dict):
        await TradeControl.save_market_data(str(t_id), md)

    async def _delete_bad(t_id: int):
        await TradeControl.delete_market_data(str(t_id))

    save_tasks = []
    delete_tasks = []

    for (t_id, sym, status, _sma, md) in results:
        if status == "OK" and md:
            eligible_tokens.append(int(t_id))
            eligible_symbols.append(str(sym))
            save_tasks.append(asyncio.create_task(_save_ok(t_id, md)))
        elif status == "FILTERED":
            delete_tasks.append(asyncio.create_task(_delete_bad(t_id)))
        else:
            if not KEEP_OLD_ON_ERROR:
                delete_tasks.append(asyncio.create_task(_delete_bad(t_id)))

    if save_tasks:
        await asyncio.gather(*save_tasks, return_exceptions=True)
    if delete_tasks:
        await asyncio.gather(*delete_tasks, return_exceptions=True)

    return eligible_tokens, eligible_symbols

# -----------------------------
# Main sync
# -----------------------------
async def run_sync() -> None:
    logger.info("Starting DHAN Async Market Data Sync (daily candles + SMA filter)...")
    logger.info(
        "Tuning: MIN_VOL_SMA=%s | CONCURRENCY=%s | MIN_REQ_INTERVAL_SEC=%.2f | LOOKBACK=%s | KEEP_OLD_ON_ERROR=%s",
        MIN_VOL_SMA, MAX_CONCURRENCY, MIN_REQ_INTERVAL_SEC, HIST_LOOKBACK_DAYS, KEEP_OLD_ON_ERROR
    )

    # Read creds from Redis
    try:
        client_id, api_secret = await TradeControl.get_config()
        access_token = (await TradeControl.get_access_token()) or api_secret
    except Exception as e:
        logger.error("❌ Redis read failed (config/token). If using self-signed Redis TLS, set REDIS_INSECURE_TLS=1")
        logger.error(f"Exception: {e}")
        return

    if not client_id or not access_token:
        logger.error("Sync aborted: DHAN client_id or access_token missing in Redis.")
        logger.error("Expected: nexus:config:api_key (client_id) and nexus:auth:access_token OR nexus:config:api_secret (access_token).")
        return

    if not STOCK_INDEX_MAPPING or len(STOCK_INDEX_MAPPING) < 10:
        logger.error("STOCK_INDEX_MAPPING looks empty/truncated. Paste your full mapping into this file.")
        return

    try:
        # 1) Build securityId mapping for our universe
        sym_to_sid = await _download_symbol_to_security_id_map(client_id, access_token)

        target: List[Tuple[int, str]] = []
        for sym in STOCK_INDEX_MAPPING.keys():
            sid = sym_to_sid.get(str(sym).upper())
            if sid:
                target.append((int(sid), str(sym).upper()))

        if SYNC_LIMIT_SYMBOLS and SYNC_LIMIT_SYMBOLS > 0:
            target = target[:SYNC_LIMIT_SYMBOLS]
            logger.info(f"SYNC_LIMIT_SYMBOLS active: limiting to {len(target)} symbols")

        logger.info(f"Universe candidates resolved: {len(target)} stocks")

        if not target:
            await TradeControl.save_subscribe_universe([], [])
            await TradeControl.set_last_sync()
            logger.warning("No target stocks resolved. Universe cleared.")
            return

        sem = asyncio.Semaphore(MAX_CONCURRENCY)

        # 2) Parallel daily historical fetch (with live progress)
        tasks = [
            asyncio.create_task(_fetch_one(client_id, access_token, str(sec_id), sym, sem))
            for (sec_id, sym) in target
        ]

        results: List[Tuple[int, str, str, float, Optional[dict]]] = []
        done = kept = filtered = failed = 0

        for fut in asyncio.as_completed(tasks):
            res = await fut
            results.append(res)

            done += 1
            _tid, _sym, status, _sma, _md = res
            if status == "OK":
                kept += 1
            elif status == "FILTERED":
                filtered += 1
            else:
                failed += 1

            if done % PROGRESS_EVERY == 0 or done == len(tasks):
                logger.info(f"Progress {done}/{len(tasks)} | kept={kept} | filtered={filtered} | errors={failed}")

        # 3) Persist + universe list
        eligible_tokens, eligible_symbols = await _persist_results(results)
        await TradeControl.save_subscribe_universe(eligible_tokens, eligible_symbols)
        await TradeControl.set_last_sync()

        # 4) Final summary
        logger.info(f"SUCCESS: eligible={len(eligible_tokens)} (SMA >= {MIN_VOL_SMA}) | filtered={filtered} | errors={failed}")
        logger.info(f"Universe tokens saved to Redis: nexus:universe:tokens (count={len(eligible_tokens)})")

        # 5) Optional: top by SMA
        if os.getenv("SYNC_PRINT_TOP", "1") == "1":
            ok_rows = [(sym, sma) for (_tid, sym, status, sma, _md) in results if status == "OK"]
            ok_rows.sort(key=lambda x: x[1], reverse=True)
            top_n = int(os.getenv("SYNC_TOP_N", "20"))
            if ok_rows:
                logger.info("Top SMA symbols:")
                for sym, sma in ok_rows[:top_n]:
                    logger.info(f"  {sym:12s} SMA={sma:.2f}")

    except Exception as e:
        logger.error(f"Critical sync failure: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(run_sync())
