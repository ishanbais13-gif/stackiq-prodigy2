import collections
import sqlite3
import threading
import time
from typing import Optional
import logging
import os

OpenAI = None  # type: ignore
_OPENAI_SDK_AVAILABLE = False
_OPENAI_SDK_IMPORT_ERROR: Optional[str] = None

try:
    from openai import OpenAI as _OpenAI  # type: ignore

    OpenAI = _OpenAI  # type: ignore
    _OPENAI_SDK_AVAILABLE = True
except Exception as e:
    try:
        import openai as _openai  # type: ignore

        _OpenAI2 = getattr(_openai, "OpenAI", None)
        if _OpenAI2 is not None:
            OpenAI = _OpenAI2  # type: ignore
            _OPENAI_SDK_AVAILABLE = True
        else:
            _OPENAI_SDK_IMPORT_ERROR = f"{type(e).__name__}:{str(e)[:180]}"
    except Exception as e2:
        _OPENAI_SDK_IMPORT_ERROR = f"{type(e2).__name__}:{str(e2)[:180]}"

from llm_config import (
    LLM_ENABLED,
    OPENAI_MODEL,
    OPENAI_MAX_OUTPUT_TOKENS,
    OPENAI_TIMEOUT_S,
    OPENAI_RETRIES,
    OPENAI_DAILY_CALL_CAP,
    llm_available as _llm_available_cfg,
)


log = logging.getLogger(__name__)


class LLMDisabledError(Exception):
    pass


class LLMCallError(Exception):
    pass


class LLMCircuitOpenError(LLMCallError):
    """Raised when the circuit breaker is open — caller should fail fast, not retry."""
    pass


class LLMDailyCapExceededError(LLMCallError):
    """Raised when OPENAI_DAILY_CALL_CAP has been reached for the current UTC day."""
    pass


_client: Optional[OpenAI] = None

_warned_missing_key = False


# ─────────────────────────────────────────────────────────────────────────────
# Circuit breaker — in-memory, process-local. Opens after a burst of failures
# and fails fast (no OpenAI call, no retries) until the cooldown elapses.
# Resetting on process restart is normal/expected circuit-breaker behavior.
# ─────────────────────────────────────────────────────────────────────────────

_CB_FAILURE_THRESHOLD = int(os.getenv("LLM_CB_FAILURE_THRESHOLD", "5"))
_CB_WINDOW_S = float(os.getenv("LLM_CB_WINDOW_S", "120"))
_CB_COOLDOWN_S = float(os.getenv("LLM_CB_COOLDOWN_S", "60"))

_cb_lock = threading.Lock()
_cb_failure_times: "collections.deque[float]" = collections.deque()
_cb_opened_until = 0.0


def llm_circuit_open() -> bool:
    with _cb_lock:
        return time.monotonic() < _cb_opened_until


def _cb_record_success() -> None:
    with _cb_lock:
        _cb_failure_times.clear()


def _cb_record_failure() -> None:
    global _cb_opened_until
    now = time.monotonic()
    with _cb_lock:
        _cb_failure_times.append(now)
        while _cb_failure_times and _cb_failure_times[0] < now - _CB_WINDOW_S:
            _cb_failure_times.popleft()
        if len(_cb_failure_times) >= _CB_FAILURE_THRESHOLD:
            _cb_opened_until = now + _CB_COOLDOWN_S
            _cb_failure_times.clear()
            log.error(
                f"LLM circuit breaker OPEN: {_CB_FAILURE_THRESHOLD}+ failures within "
                f"{_CB_WINDOW_S:.0f}s — failing fast for {_CB_COOLDOWN_S:.0f}s"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Usage logging + daily call cap — persisted to sqlite so both survive a
# process restart (an in-memory-only counter would silently reset the cap
# every time Railway restarts the service).
# ─────────────────────────────────────────────────────────────────────────────

_USAGE_DB_PATH = os.getenv(
    "LLM_USAGE_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "llm_usage.db"),
)
_usage_lock = threading.Lock()
_usage_schema_ready = False


def _usage_conn() -> sqlite3.Connection:
    global _usage_schema_ready
    conn = sqlite3.connect(_USAGE_DB_PATH, timeout=5, check_same_thread=False)
    if not _usage_schema_ready:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS llm_calls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                label TEXT,
                user_id INTEGER,
                model TEXT,
                success INTEGER NOT NULL,
                error TEXT,
                latency_ms INTEGER,
                prompt_tokens INTEGER,
                completion_tokens INTEGER
            )"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_llm_calls_ts ON llm_calls(ts)")
        conn.commit()
        _usage_schema_ready = True
    return conn


def _log_llm_call(
    *,
    label: str,
    user_id: Optional[int],
    model: str,
    success: bool,
    error: str = "",
    latency_ms: int = 0,
    prompt_tokens: Optional[int] = None,
    completion_tokens: Optional[int] = None,
) -> None:
    try:
        with _usage_lock:
            conn = _usage_conn()
            conn.execute(
                "INSERT INTO llm_calls "
                "(ts, label, user_id, model, success, error, latency_ms, prompt_tokens, completion_tokens) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    int(time.time()), label, user_id, model, 1 if success else 0,
                    (error or "")[:300], latency_ms, prompt_tokens, completion_tokens,
                ),
            )
            conn.commit()
            conn.close()
    except Exception:
        log.warning("llm_usage: failed to log call", exc_info=True)


def _utc_midnight_ts() -> int:
    now = time.time()
    return int(now - (now % 86400))


def llm_calls_today() -> int:
    try:
        with _usage_lock:
            conn = _usage_conn()
            row = conn.execute(
                "SELECT COUNT(*) FROM llm_calls WHERE ts >= ?", (_utc_midnight_ts(),)
            ).fetchone()
            conn.close()
        return int(row[0]) if row else 0
    except Exception:
        return 0


def _daily_cap_exceeded() -> bool:
    if OPENAI_DAILY_CALL_CAP <= 0:
        return False
    return llm_calls_today() >= OPENAI_DAILY_CALL_CAP


def llm_available() -> bool:
    return bool(_OPENAI_SDK_AVAILABLE) and bool(_llm_available_cfg())


def init_llm_client() -> bool:
    """Initialize and validate OpenAI client once. Logs status; never raises."""
    try:
        strict = False
        try:
            strict = str(os.getenv("STACKIQ_LLM_STRICT", "0") or "0").strip().lower() in ("1", "true", "yes", "on")
        except Exception:
            strict = False

        if not LLM_ENABLED:
            msg = "llm_disabled: config_disabled"
            if strict:
                log.error(msg)
            else:
                log.warning(msg)
            if strict:
                raise RuntimeError(msg)
            return False

        if not _OPENAI_SDK_AVAILABLE:
            msg = f"llm_disabled: sdk_import_error={(_OPENAI_SDK_IMPORT_ERROR or 'ImportError')}"
            if strict:
                log.error(msg)
            else:
                log.warning(msg)
            if strict:
                raise RuntimeError(msg)
            return False

        api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
        if not api_key:
            msg = "llm_disabled: missing_api_key"
            if strict:
                log.error(msg)
            else:
                log.warning(msg)
            if strict:
                raise RuntimeError(msg)
            return False

        if not llm_available():
            msg = "llm_disabled: config_gating"
            if strict:
                log.error(msg)
            else:
                log.warning(msg)
            if strict:
                raise RuntimeError(msg)
            return False
        _ = _get_client()
        log.info("LLM client initialized successfully")
        return True
    except Exception as e:
        try:
            if strict:
                log.error(f"LLM client initialization failed: {e}")
            else:
                log.warning(f"LLM client initialization failed: {e}")
        except Exception:
            pass
        return False


def _get_client() -> OpenAI:
    global _client
    if not _OPENAI_SDK_AVAILABLE:
        raise LLMDisabledError(f"sdk_import_error:{(_OPENAI_SDK_IMPORT_ERROR or 'unknown')}")
    if _client is None:
        api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
        if not api_key:
            raise LLMDisabledError("missing_api_key")
        # Pass api_key explicitly to avoid env ordering issues.
        _client = OpenAI(api_key=api_key)
    return _client


def call_llm_text(
    *,
    system: str,
    user: str,
    model: str = OPENAI_MODEL,
    max_output_tokens: int = OPENAI_MAX_OUTPUT_TOKENS,
    timeout_s: float = OPENAI_TIMEOUT_S,
    label: str = "unknown",
    user_id: Optional[int] = None,
) -> str:
    """
    Returns plain text from the Responses API.
    Uses short timeouts + light retries so /analyze never hangs forever.

    `label` identifies the calling feature (e.g. "chat", "news_sentiment")
    and `user_id` the requesting user, if any -- both are for usage logging
    only, not gating. Every top-level call (win or lose, after retries) is
    logged once to llm_usage.db; the circuit breaker and daily cap are
    checked before any network call is attempted.
    """
    global _warned_missing_key
    if not LLM_ENABLED:
        raise LLMDisabledError("config_disabled")
    if not _OPENAI_SDK_AVAILABLE:
        raise LLMDisabledError(f"sdk_import_error:{(_OPENAI_SDK_IMPORT_ERROR or 'unknown')}")
    if not llm_available():
        if not _warned_missing_key:
            _warned_missing_key = True
            try:
                if not (os.getenv("OPENAI_API_KEY") or "").strip():
                    log.warning("llm_disabled: missing_api_key")
                else:
                    log.warning("llm_disabled: config_gating")
            except Exception:
                pass
        if not (os.getenv("OPENAI_API_KEY") or "").strip():
            raise LLMDisabledError("missing_api_key")
        raise LLMDisabledError("config_gating")

    if llm_circuit_open():
        raise LLMCircuitOpenError("circuit_breaker_open")

    if _daily_cap_exceeded():
        raise LLMDailyCapExceededError(f"daily_call_cap_reached:{OPENAI_DAILY_CALL_CAP}")

    call_t0 = time.monotonic()
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    last_err: Optional[Exception] = None
    for attempt in range(0, max(1, OPENAI_RETRIES + 1)):
        try:
            client = _get_client()

            text = ""
            # Prefer Responses API when available, but fall back to Chat Completions
            # for older SDKs.
            try:
                if hasattr(client, "responses") and getattr(client, "responses") is not None:
                    resp = client.responses.create(
                        model=model,
                        input=[
                            {"role": "system", "content": system},
                            {"role": "user", "content": user},
                        ],
                        temperature=0.2,
                        max_output_tokens=max_output_tokens,
                        timeout=float(timeout_s),
                    )
                    text = str(getattr(resp, "output_text", "") or "").strip()
                    usage = getattr(resp, "usage", None)
                    if usage is not None:
                        prompt_tokens = getattr(usage, "input_tokens", None) or getattr(usage, "prompt_tokens", None)
                        completion_tokens = getattr(usage, "output_tokens", None) or getattr(usage, "completion_tokens", None)
                else:
                    raise AttributeError("responses_api_unavailable")
            except Exception:
                # Some newer models reject `max_tokens` and require `max_completion_tokens`.
                resp2 = None
                try:
                    resp2 = client.chat.completions.create(
                        model=model,
                        messages=[
                            {"role": "system", "content": system},
                            {"role": "user", "content": user},
                        ],
                        temperature=0.2,
                        max_completion_tokens=int(max_output_tokens),
                        timeout=float(timeout_s),
                    )
                except TypeError:
                    resp2 = client.chat.completions.create(
                        model=model,
                        messages=[
                            {"role": "system", "content": system},
                            {"role": "user", "content": user},
                        ],
                        temperature=0.2,
                        max_tokens=int(max_output_tokens),
                        timeout=float(timeout_s),
                    )
                try:
                    text = str(resp2.choices[0].message.content or "").strip()
                except Exception:
                    text = ""
                try:
                    usage2 = getattr(resp2, "usage", None)
                    if usage2 is not None:
                        prompt_tokens = getattr(usage2, "prompt_tokens", None)
                        completion_tokens = getattr(usage2, "completion_tokens", None)
                except Exception:
                    pass
            if not text:
                raise LLMCallError("Empty LLM response")

            _cb_record_success()
            _log_llm_call(
                label=label, user_id=user_id, model=model, success=True,
                latency_ms=int((time.monotonic() - call_t0) * 1000),
                prompt_tokens=prompt_tokens, completion_tokens=completion_tokens,
            )
            return text

        except Exception as e:
            last_err = e
            backoff = min(2 ** (attempt + 1), 8)
            try:
                log.warning(f"LLM retry attempt {attempt + 1}/{max(1, OPENAI_RETRIES)}: {type(e).__name__}: {str(e)[:120]} — retrying in {backoff}s")
            except Exception:
                pass
            if attempt < max(0, OPENAI_RETRIES):
                time.sleep(backoff)

    try:
        log.error(f"LLM failed after retries: {type(last_err).__name__}: {str(last_err)[:200]}")
    except Exception:
        pass
    _cb_record_failure()
    _log_llm_call(
        label=label, user_id=user_id, model=model, success=False,
        error=f"{type(last_err).__name__}: {str(last_err)[:200]}",
        latency_ms=int((time.monotonic() - call_t0) * 1000),
    )
    raise LLMCallError(f"LLM call failed after retries: {last_err}")
