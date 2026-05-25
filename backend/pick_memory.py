from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict
import threading


_LAST_PICK_LOCK = threading.Lock()
LAST_PICK: Dict[str, Any] = {
    "symbol": None,
    "first_seen": None,
    "last_seen": None,
    "streak_scans": 0,
}


def update_pick_memory(symbol: str) -> dict:
    sym = str(symbol or "").strip().upper()
    now = datetime.now(timezone.utc)

    with _LAST_PICK_LOCK:
        prev_symbol = str(LAST_PICK.get("symbol") or "").strip().upper()
        first_seen = LAST_PICK.get("first_seen")

        if sym and sym == prev_symbol:
            LAST_PICK["streak_scans"] = int(LAST_PICK.get("streak_scans") or 0) + 1
            LAST_PICK["last_seen"] = now
        else:
            LAST_PICK["symbol"] = sym or None
            LAST_PICK["first_seen"] = now
            LAST_PICK["last_seen"] = now
            LAST_PICK["streak_scans"] = 1 if sym else 0

        fs = LAST_PICK.get("first_seen")
        age_minutes = 0.0
        try:
            if isinstance(fs, datetime):
                age_minutes = max(0.0, (now - fs).total_seconds() / 60.0)
        except Exception:
            age_minutes = 0.0

        return {
            "pick_streak_scans": int(LAST_PICK.get("streak_scans") or 0),
            "pick_age_minutes": float(round(age_minutes, 2)),
        }
