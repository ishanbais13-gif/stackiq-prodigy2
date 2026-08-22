"""
push.py — Native iOS push notifications (APNs) for new-pick alerts.

Environment variables needed:
  APNS_KEY_ID       — Key ID of the APNs Authentication Key (.p8)
  APNS_TEAM_ID      — Apple Developer Team ID (same value as auth.py's APPLE_TEAM_ID)
  APNS_PRIVATE_KEY  — contents of the .p8 file (\\n-escaped, same convention as APPLE_PRIVATE_KEY)
  APNS_BUNDLE_ID    — push topic, same value as the app's bundle id (APPLE_BUNDLE_ID)
  APNS_ENVIRONMENT  — "sandbox" or "production" (default: production)

Unlike email/SMS alerts (alerts.py), push is open to every plan at the
registration/permission level -- there's no paid gate on who can receive a
push. What differs by plan is the *content*: paid users get the real pick
(ticker, direction, entry -- same information the email/SMS alert carries),
free users get a teaser that deep-links to the upgrade screen instead of a
pick they can't see.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List

from jose import jwt

log = logging.getLogger("stackiq")

_AUTH_DB_PATH = os.getenv("AUTH_DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "auth.db"))

APNS_KEY_ID      = os.getenv("APNS_KEY_ID", "")
APNS_TEAM_ID     = os.getenv("APNS_TEAM_ID", "")
APNS_PRIVATE_KEY = os.getenv("APNS_PRIVATE_KEY", "").replace("\\n", "\n")
APNS_BUNDLE_ID   = os.getenv("APNS_BUNDLE_ID", "")
APNS_ENVIRONMENT = os.getenv("APNS_ENVIRONMENT", "production").strip().lower()

_APNS_HOST = (
    "https://api.sandbox.push.apple.com"
    if APNS_ENVIRONMENT == "sandbox"
    else "https://api.push.apple.com"
)

# Provider token is valid up to 1hr per Apple's docs -- cache and refresh
# well under that so an in-flight send never races an expiry.
_provider_token_cache: Dict[str, Any] = {"token": None, "issued_at": 0.0}


def _provider_token() -> str:
    now = time.time()
    if _provider_token_cache["token"] and (now - _provider_token_cache["issued_at"]) < 2700:  # 45 min
        return _provider_token_cache["token"]
    payload = {"iss": APNS_TEAM_ID, "iat": int(now)}
    token = jwt.encode(payload, APNS_PRIVATE_KEY, algorithm="ES256", headers={"kid": APNS_KEY_ID})
    _provider_token_cache["token"] = token
    _provider_token_cache["issued_at"] = now
    return token


def _get_all_device_tokens() -> List[Dict[str, Any]]:
    """Every registered device across every plan -- push isn't paid-gated at registration."""
    try:
        conn = sqlite3.connect(_AUTH_DB_PATH, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT d.device_token, u.plan, u.subscription_status
            FROM device_tokens d
            JOIN users u ON u.id = d.user_id
            """
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        log.warning(f"push.get_tokens: {e}")
        return []


def _delete_device_token(token: str) -> None:
    try:
        conn = sqlite3.connect(_AUTH_DB_PATH, check_same_thread=False, timeout=30)
        conn.execute("DELETE FROM device_tokens WHERE device_token = ?", (token,))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"push.delete_token: {e}")


def _send_apns_verbose(device_token: str, title: str, body: str, data: Dict[str, Any]):
    """Returns (ok, detail). detail is the APNs status/reason or a local
    description -- used by the admin test-push endpoint so a failure is
    diagnosable instead of only visible in server logs."""
    if not (APNS_KEY_ID and APNS_TEAM_ID and APNS_PRIVATE_KEY and APNS_BUNDLE_ID):
        return False, "APNs env vars not fully configured"
    import httpx

    payload = {
        "aps": {"alert": {"title": title, "body": body}, "sound": "default"},
        **data,
    }
    headers = {
        "authorization": f"bearer {_provider_token()}",
        "apns-topic": APNS_BUNDLE_ID,
        "apns-push-type": "alert",
        "apns-priority": "10",
    }
    try:
        with httpx.Client(http2=True, timeout=10) as client:
            resp = client.post(f"{_APNS_HOST}/3/device/{device_token}", json=payload, headers=headers)
        if resp.status_code == 200:
            return True, "delivered to APNs"
        reason = ""
        try:
            reason = resp.json().get("reason", "")
        except Exception:
            pass
        log.warning(f"push.send failed status={resp.status_code} reason={reason} token={device_token[:12]}...")
        # BadDeviceToken/Unregistered both mean this token is permanently
        # dead -- prune it so the table self-cleans without a separate job.
        if resp.status_code == 410 or reason == "BadDeviceToken":
            _delete_device_token(device_token)
        return False, f"HTTP {resp.status_code} {reason}".strip()
    except Exception as e:
        log.warning(f"push.send exception: {e}")
        return False, f"exception: {e}"


def _send_apns(device_token: str, title: str, body: str, data: Dict[str, Any]) -> None:
    _send_apns_verbose(device_token, title, body, data)


def _paid_push_payload(pick: Dict[str, Any]):
    """Same underlying pick data alerts.py's email/SMS path uses -- see alerts._fire_new_pick."""
    symbol = str(pick.get("symbol") or "").strip().upper()
    decision = str(pick.get("trade_decision") or pick.get("decision") or "").upper()
    tp = pick.get("trade_plan") or {}
    entry = tp.get("entry") or pick.get("entry")
    try:
        entry = float(entry) if entry else None
    except Exception:
        entry = None

    decision_label = decision.replace("_", " ").title() if decision else "New Setup"
    title = f"Today's Pick: ${symbol}"
    body = f"{decision_label} — entry ${entry:.2f}" if entry else decision_label
    data = {"type": "pick", "symbol": symbol}
    return title, body, data


def _free_push_payload():
    # Copy matches the existing free-tier gate wording in App.jsx
    # ("You've used your free pick for this month. Upgrade to see every
    # daily pick.") so the message is consistent wherever a free user sees it.
    title = "New pick just dropped \U0001F512"
    body = "You've used your free pick for this month — upgrade to see it."
    data = {"type": "upgrade"}
    return title, body, data


def _fire_new_pick_push(pick: Dict[str, Any]) -> None:
    symbol = str(pick.get("symbol") or "").strip().upper()
    if not symbol:
        return

    devices = _get_all_device_tokens()
    log.info(f"push.new_pick: {symbol} -> {len(devices)} registered devices")

    paid_title, paid_body, paid_data = _paid_push_payload(pick)
    free_title, free_body, free_data = _free_push_payload()

    for d in devices:
        plan = str(d.get("plan") or "free").lower()
        sub_status = str(d.get("subscription_status") or "").lower()
        is_paid = plan in ("starter", "pro", "elite") and sub_status == "active"
        token = d.get("device_token")
        if not token:
            continue
        if is_paid:
            _send_apns(token, paid_title, paid_body, paid_data)
        else:
            _send_apns(token, free_title, free_body, free_data)


def send_new_pick_push_bg(pick: Dict[str, Any]) -> None:
    """Fire-and-forget push notification in a background thread."""
    threading.Thread(target=_fire_new_pick_push, args=(pick,), daemon=True).start()
