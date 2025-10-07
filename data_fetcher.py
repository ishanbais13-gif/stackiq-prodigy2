# data_fetcher.py
# StackIQ — Finnhub client (quotes + candles) with sandbox toggle, retries, and clear errors.

import os
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv

# --- Env & logging -----------------------------------------------------------
load_dotenv()
logger = logging.getLogger("stackiq.data")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

FINNHUB_API_KEY: str = os.getenv("FINNHUB_API_KEY", "").strip()
USE_SANDBOX: bool = os.getenv("FINNHUB_SANDBOX", "false").lower() == "true"
# Optional manual override, else we pick sandbox vs prod automatically
BASE_URL: str = os.getenv(
    "FINNHUB_BASE_URL",
    "https://sandbox.finnhub.io/api/v1" if USE_SANDBOX else "https://finnhub.io/api/v1",
)

# Tunables (safe defaults)
REQ_TIMEOUT: int = int(os.getenv("FINNHUB_TIMEOUT", "15"))     # seconds
MAX_RETRIES: int = int(os.getenv("FINNHUB_RETRIES", "2"))      # 0 = no retry
RETRY_BACKOFF: float = float(os.getenv("FINNHUB_BACKOFF", "0.75"))  # seconds base

# --- Basic validation ---------------------------------------------------------
if not FINNHUB_API_KEY:
    # We raise now so failures are obvious in Log Stream on boot.
    raise RuntimeError(
        "FINNHUB_API_KEY is not set. Add it in Azure → App Service → Configuration → Application settings."
    )

logger.info(f"Finnhub BASE_URL={BASE_URL}  SANDBOX={USE_SANDBOX}  TIMEOUT={REQ_TIMEOUT}s  RETRIES={MAX_RETRIES}")


# --- Internals ----------------------------------------------------------------
_session = requests.Session()

def _normalize_symbol(symbol: str) -> str:
    """Uppercase & strip spaces."""
    return (symbol or "").strip().upper()

def _should_retry(status_code: int) -> bool:
    """Retry on rate-limit/temporary errors."""
    return status_code in (429, 500, 502, 503, 504)

def _request(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """GET wrapper with retries and clear error messages."""
    if "token" not in params:
        params = {**params, "token": FINNHUB_API_KEY}

    url = f"{BASE_URL}{path}"
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = _session.get(url, params=params, timeout=REQ_TIMEOUT)
        except requests.RequestException as e:
            # network-level (DNS, socket timeout, etc.)
            if attempt <= MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)
                continue
            raise requests.HTTPError(f"Network error reaching Finnhub: {e}") from e

        if resp.status_code >= 400:
            # Include Finnhub's body to make it obvious (403 "no access", 429 "rate limit", etc.)
            body = (resp.text or "").strip()
            if _should_retry(resp.status_code) and attempt <= MAX_RETRIES:
                time.sleep(RETRY_BACKOFF * attempt)
                continue
            raise requests.HTTPError(f"{resp.status_code} from Finnhub: {body}")

        # Successful HTTP — return JSON (Finnhub always returns JSON here)
        return resp.json()


def _build_time_window(
    resolution: str = "D",
    count: int = 60,
    to_ts: Optional[int] = None,
    from_ts: Optional[int] = None,
) -> (int, int):
    """
    Build sane from/to UNIX timestamps.
    - For daily ('D'), widen by 2x to cover weekends/holidays.
    - For minute resolutions ('1','5','15','60','240'), widen by 2x.
    - Accepts 'D', 'W', 'M' too.
    """
    now = datetime.now(timezone.utc)

    if to_ts is None:
        to_ts = int(now.timestamp())

    if from_ts is None:
        # Choose a window big enough that Finnhub returns data even with market closures.
        res = (resolution or "D").strip().upper()
        if res == "D":
            delta = timedelta(days=count * 2)
        elif res == "W":
            delta = timedelta(weeks=count * 2)
        elif res == "M":
            # crude month estimate is fine for windowing
            delta = timedelta(days=30 * count * 2)
        else:
            # try numeric minutes
            try:
                minutes = int(resolution)
                delta = timedelta(minutes=minutes * count * 2)
            except (TypeError, ValueError):
                # default fallback
                delta = timedelta(days=count * 2)
        from_ts = int((now - delta).timestamp())

    return int(from_ts), int(to_ts)


# --- Public API ---------------------------------------------------------------
def get_quote(symbol: str) -> Dict[str, Any]:
    """
    Get last price/quote (c/h/l/o/pc/t).
    Works on free, sandbox, and paid Finnhub plans.
    """
    sym = _normalize_symbol(symbol)
    return _request("/quote", {"symbol": sym})


def get_candles(
    symbol: str,
    resolution: str = "D",
    count: int = 60,
    *,
    from_ts: Optional[int] = None,
    to_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Get OHLCV candles.
    - resolution: 'D','W','M' or minute string like '1','5','15','60','240'
    - count: number of bars to *aim* for (we widen the time window to avoid 'no_data')
    - returns Finnhub's JSON (keys: s,c,h,l,o,t,v). If plan forbids this, Finnhub returns 403.
    """
    sym = _normalize_symbol(symbol)
    f_ts, t_ts = _build_time_window(resolution=resolution, count=count, to_ts=to_ts, from_ts=from_ts)

    data = _request("/stock/candle", {
        "symbol": sym,
        "resolution": resolution,
        "from": f_ts,
        "to": t_ts,
    })

    # Finnhub returns { "s": "no_data" } with 200 OK sometimes. Keep that shape.
    # Caller can check data.get("s") == "ok"
    return data


def get_company_profile(symbol: str) -> Dict[str, Any]:
    """
    Company profile (lightweight). Helpful for sanity checks in UI.
    """
    sym = _normalize_symbol(symbol)
    return _request("/stock/profile2", {"symbol": sym})


# Optional: simple helper that guarantees "ok" candles or raises a friendly error.
def get_candles_or_explain(symbol: str, resolution: str = "D", count: int = 60) -> Dict[str, Any]:
    """
    Fetch candles and either return them (status 'ok') or raise a clear exception
    explaining likely cause (e.g., plan limitation).
    """
    data = get_candles(symbol, resolution, count)
    status = data.get("s")
    if status == "ok":
        return data
    if status == "no_data":
        raise ValueError(f"No data for {symbol} at resolution={resolution}. Try a wider window or another symbol.")
    # Unexpected shape — return raw for debugging
    raise ValueError(f"Unexpected candles response for {symbol}: {data}")









































