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
    llm_available as _llm_available_cfg,
)


log = logging.getLogger(__name__)


class LLMDisabledError(Exception):
    pass


class LLMCallError(Exception):
    pass


_client: Optional[OpenAI] = None

_warned_missing_key = False


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
            log.error(msg)
            if strict:
                raise RuntimeError(msg)
            return False

        if not _OPENAI_SDK_AVAILABLE:
            msg = f"llm_disabled: sdk_import_error={(_OPENAI_SDK_IMPORT_ERROR or 'ImportError')}"
            log.error(msg)
            if strict:
                raise RuntimeError(msg)
            return False

        api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
        if not api_key:
            msg = "llm_disabled: missing_api_key"
            log.error(msg)
            if strict:
                raise RuntimeError(msg)
            return False

        if not llm_available():
            msg = "llm_disabled: config_gating"
            log.error(msg)
            if strict:
                raise RuntimeError(msg)
            return False
        _ = _get_client()
        log.info("LLM client initialized successfully")
        return True
    except Exception as e:
        try:
            log.error(f"LLM client initialization failed: {e}")
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
) -> str:
    """
    Returns plain text from the Responses API.
    Uses short timeouts + light retries so /analyze never hangs forever.
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
            if not text:
                raise LLMCallError("Empty LLM response")
            return text

        except Exception as e:
            last_err = e
            time.sleep(0.25 * (attempt + 1))

    raise LLMCallError(f"LLM call failed after retries: {last_err}")
