from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional


def _clamp_0_100(v: Any) -> float:
    try:
        x = float(v)
    except Exception:
        x = 0.0
    if x < 0.0:
        x = 0.0
    if x > 100.0:
        x = 100.0
    return float(x)


def _safe_zoneinfo(tz: Optional[str]) -> ZoneInfo:
    try:
        if tz:
            return ZoneInfo(str(tz))
    except Exception:
        pass
    return ZoneInfo("America/New_York")


def _format_window(user_tz: Optional[str], start_h: int, start_m: int, end_h: int, end_m: int) -> str:
    tz = _safe_zoneinfo(user_tz)
    now_local = datetime.now(timezone.utc).astimezone(tz)
    start = now_local.replace(hour=int(start_h), minute=int(start_m), second=0, microsecond=0)
    end = now_local.replace(hour=int(end_h), minute=int(end_m), second=0, microsecond=0)
    return f"{start.strftime('%-I:%M %p')} – {end.strftime('%-I:%M %p')}"


def build_execution_plan(*, indicators: Dict[str, Any], tz: Optional[str]) -> Dict[str, Any]:
    mom = _clamp_0_100((indicators or {}).get("momentum"))
    tr = _clamp_0_100((indicators or {}).get("trend"))
    vol = _clamp_0_100((indicators or {}).get("volatility"))
    liq = _clamp_0_100((indicators or {}).get("liquidity"))

    if liq < 35.0:
        return {
            "strategy": "AVOID_LOW_LIQUIDITY",
            "time_window": "Next session",
            "session": "Pre-market",
            "playbook": "Avoid / Next session",
            "timezone": str(_safe_zoneinfo(tz).key),
        }

    if (tr >= 70.0 and mom >= 70.0):
        return {
            "strategy": "BREAKOUT",
            "time_window": _format_window(tz, 9, 45, 11, 30),
            "session": "Open",
            "playbook": "Breakout strategy",
            "timezone": str(_safe_zoneinfo(tz).key),
        }

    if vol >= 75.0:
        return {
            "strategy": "SCALP",
            "time_window": _format_window(tz, 13, 30, 15, 0),
            "session": "Midday",
            "playbook": "Scalp window",
            "timezone": str(_safe_zoneinfo(tz).key),
        }

    return {
        "strategy": "PULLBACK_RECLAIM",
        "time_window": _format_window(tz, 9, 45, 11, 30),
        "session": "Open",
        "playbook": "Pullback / Reclaim",
        "timezone": str(_safe_zoneinfo(tz).key),
    }
