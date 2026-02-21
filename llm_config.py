import os


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "t", "yes", "y", "on")


# Toggle LLM on/off from env (matches your "LLM disabled" behavior, but makes it explicit)
LLM_ENABLED: bool = _env_bool("STACKIQ_LLM_ENABLED", default=True)

def _openai_api_key() -> str:
    # Read dynamically so loading .env after imports still works.
    return (os.getenv("OPENAI_API_KEY") or "").strip()

# Model and limits
OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_MAX_OUTPUT_TOKENS: int = int(os.getenv("OPENAI_MAX_OUTPUT_TOKENS", "400"))

# Timeouts / retries (keep conservative so you don’t hang UI)
OPENAI_TIMEOUT_S: float = float(os.getenv("OPENAI_TIMEOUT_S", "8"))
OPENAI_RETRIES: int = int(os.getenv("OPENAI_RETRIES", "1"))

# Optional: hard cap to protect costs if you want it later
OPENAI_DAILY_CALL_CAP: int = int(os.getenv("OPENAI_DAILY_CALL_CAP", "0"))  # 0 = disabled


def llm_available() -> bool:
    return bool(_openai_api_key()) and bool(LLM_ENABLED)
