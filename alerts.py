"""
alerts.py — New-pick and outcome alerts via email (SendGrid) and SMS (Twilio).

Environment variables needed:
  SENDGRID_API_KEY       — already set (shared with auth.py)
  ALERT_FROM_EMAIL       — already set (shared with auth.py)
  TWILIO_ACCOUNT_SID     — Twilio Account SID
  TWILIO_AUTH_TOKEN      — Twilio Auth Token
  TWILIO_FROM_NUMBER     — Twilio "From" number, E.164 format (e.g. +15551234567)
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import urllib.request as _ur
import urllib.error
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("stackiq")

_AUTH_DB_PATH   = os.getenv("AUTH_DB_PATH",  os.path.join(os.path.dirname(os.path.abspath(__file__)), "auth.db"))
_FROM_EMAIL     = os.getenv("ALERT_FROM_EMAIL", "hello@useaurexis.com")
_FRONTEND_URL   = os.getenv("FRONTEND_ORIGIN",  "https://useaurexis.com")


def _sg_key() -> str:
    return os.getenv("SENDGRID_API_KEY", "")


def _twilio_creds():
    return (
        os.getenv("TWILIO_ACCOUNT_SID", ""),
        os.getenv("TWILIO_AUTH_TOKEN", ""),
        os.getenv("TWILIO_FROM_NUMBER", ""),
    )


# ─────────────────────────────────────────────────────────────────────────────
# DB migration — add alert columns to existing users table
# ─────────────────────────────────────────────────────────────────────────────

def migrate_alerts_columns() -> None:
    """Non-destructive migration — safe to call on every startup."""
    try:
        conn = sqlite3.connect(_AUTH_DB_PATH, check_same_thread=False)
        for col, defn in [
            ("phone",            "TEXT"),
            ("phone_verified",   "INTEGER NOT NULL DEFAULT 0"),
            ("alerts_new_pick",  "INTEGER NOT NULL DEFAULT 1"),
            ("alerts_outcome",   "INTEGER NOT NULL DEFAULT 1"),
            ("alerts_channel",   "TEXT NOT NULL DEFAULT 'email'"),  # email | sms | both
        ]:
            try:
                conn.execute(f"ALTER TABLE users ADD COLUMN {col} {defn}")
            except Exception:
                pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS phone_otp_tokens (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER NOT NULL,
                phone      TEXT    NOT NULL,
                code       TEXT    NOT NULL,
                expires_at TEXT    NOT NULL,
                used       INTEGER NOT NULL DEFAULT 0,
                attempts   INTEGER NOT NULL DEFAULT 0
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"alerts.migrate: {e}")


migrate_alerts_columns()


# ─────────────────────────────────────────────────────────────────────────────
# Fetch opted-in users
# ─────────────────────────────────────────────────────────────────────────────

def _get_opted_in_users(alert_col: str) -> List[Dict[str, Any]]:
    """Return every user opted in to the given alert column, across all plans.
    Callers apply their own per-channel gating: email stays paid-only (unchanged),
    SMS is open to all opted-in + phone-verified users with plan-based content,
    mirroring push.py's paid-vs-teaser pattern."""
    try:
        conn = sqlite3.connect(_AUTH_DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"SELECT email, first_name, phone, phone_verified, alerts_channel, plan, subscription_status "
            f"FROM users WHERE {alert_col} = 1"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        log.warning(f"alerts.get_users: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Low-level email sender (SendGrid)
# ─────────────────────────────────────────────────────────────────────────────

def _send_email(to_email: str, subject: str, html: str) -> bool:
    key = _sg_key()
    if not key:
        log.warning("alerts: SENDGRID_API_KEY not set — cannot send email to %s", to_email)
        return False
    try:
        payload = json.dumps({
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": _FROM_EMAIL, "name": "Aurexis"},
            "subject": subject,
            "content": [{"type": "text/html", "value": html}],
        }).encode()
        req = _ur.Request(
            "https://api.sendgrid.com/v3/mail/send",
            data=payload,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            method="POST",
        )
        with _ur.urlopen(req, timeout=10) as resp:
            log.info("alerts.email: sent to %s (HTTP %s)", to_email, resp.status)
            return resp.status in (200, 202)
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        log.error("alerts.email: HTTP %s sending to %s — %s", e.code, to_email, body)
        return False
    except Exception as e:
        log.error("alerts.email: unexpected error sending to %s — %s", to_email, e)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Low-level SMS/WhatsApp sender (Twilio)
# ─────────────────────────────────────────────────────────────────────────────

def _twilio_send(to_phone: str, body: str) -> Tuple[bool, str]:
    """Returns (ok, error_message). error_message is human-readable and safe
    to show a user (Twilio's own message text, or a short local description)."""
    account_sid, auth_token, from_number = _twilio_creds()
    if not (account_sid and auth_token and from_number):
        log.warning("alerts.sms: Twilio creds not set — cannot send SMS to %s", to_phone)
        return False, "SMS is not configured."
    if not to_phone or not to_phone.startswith("+"):
        log.warning("alerts.sms: invalid phone number %r", to_phone)
        return False, "Invalid phone number."
    try:
        import base64
        url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
        payload = urllib.parse.urlencode({"To": to_phone, "From": from_number, "Body": body}).encode("utf-8")
        basic_auth = base64.b64encode(f"{account_sid}:{auth_token}".encode("utf-8")).decode("ascii")
        req = _ur.Request(
            url,
            data=payload,
            headers={
                "Authorization": f"Basic {basic_auth}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            method="POST",
        )
        with _ur.urlopen(req, timeout=10) as resp:
            resp_body = json.loads(resp.read().decode("utf-8", errors="replace"))
            msg_sid = resp_body.get("sid", "")
            log.info("alerts.sms: sent to %s (HTTP %s, sid=%s)", to_phone, resp.status, msg_sid)
            ok = resp.status in (200, 201) and bool(msg_sid)
            return ok, "" if ok else "Twilio did not confirm delivery."
    except urllib.error.HTTPError as e:
        err_body = ""
        try:
            err_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        log.error("alerts.sms: HTTP %s sending to %s — %s", e.code, to_phone, err_body)
        msg = "Couldn't send — check the number and try again."
        try:
            msg = json.loads(err_body).get("message") or msg
        except Exception:
            pass
        return False, msg
    except Exception as e:
        log.error("alerts.sms: unexpected error sending to %s — %s", to_phone, e)
        return False, "Couldn't send — try again."


def _send_sms(to_phone: str, body: str) -> bool:
    return _twilio_send(to_phone, body)[0]


# ─────────────────────────────────────────────────────────────────────────────
# Phone verification (OTP over SMS) — mirrors auth.py's email OTP pattern
# ─────────────────────────────────────────────────────────────────────────────

import hmac as _hmac
import secrets as _secrets
import time as _time
from datetime import datetime, timedelta, timezone

_PHONE_OTP_EXPIRE_MINUTES = 10
_PHONE_OTP_MAX_ATTEMPTS   = 10

_phone_otp_resend_attempts: dict[int, list[float]] = {}  # user_id → list of epoch timestamps
_PHONE_OTP_RESEND_MAX    = 5
_PHONE_OTP_RESEND_WINDOW = 600  # 10 minutes


def phone_otp_resend_rate_ok(user_id: int) -> bool:
    now = _time.time()
    timestamps = [t for t in _phone_otp_resend_attempts.get(user_id, []) if now - t < _PHONE_OTP_RESEND_WINDOW]
    _phone_otp_resend_attempts[user_id] = timestamps
    if len(timestamps) >= _PHONE_OTP_RESEND_MAX:
        return False
    _phone_otp_resend_attempts[user_id].append(now)
    return True


def _generate_phone_otp(user_id: int, phone: str) -> str:
    code = f"{_secrets.randbelow(1_000_000):06d}"
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=_PHONE_OTP_EXPIRE_MINUTES)).isoformat()
    conn = sqlite3.connect(_AUTH_DB_PATH, check_same_thread=False)
    try:
        # Invalidate any previous unused codes for this user
        conn.execute("UPDATE phone_otp_tokens SET used = 1 WHERE user_id = ? AND used = 0", (user_id,))
        conn.execute(
            "INSERT INTO phone_otp_tokens (user_id, phone, code, expires_at) VALUES (?, ?, ?, ?)",
            (user_id, phone, code, expires_at),
        )
        conn.commit()
    finally:
        conn.close()
    return code


def verify_phone_otp(user_id: int, phone: str, code: str) -> bool:
    conn = sqlite3.connect(_AUTH_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        active = conn.execute(
            "SELECT id, phone, code, expires_at, attempts FROM phone_otp_tokens "
            "WHERE user_id = ? AND used = 0 ORDER BY id DESC LIMIT 1",
            (user_id,),
        ).fetchone()
        if not active:
            return False
        if datetime.fromisoformat(active["expires_at"]) < datetime.now(timezone.utc):
            conn.execute("UPDATE phone_otp_tokens SET used = 1 WHERE id = ?", (active["id"],))
            conn.commit()
            return False
        attempts = int(active["attempts"] or 0)
        if attempts >= _PHONE_OTP_MAX_ATTEMPTS:
            conn.execute("UPDATE phone_otp_tokens SET used = 1 WHERE id = ?", (active["id"],))
            conn.commit()
            return False
        # The code must match AND still be for the phone number currently on file --
        # guards against a stale code confirming a number the user has since changed.
        if not _hmac.compare_digest(str(active["code"]), str(code)) or active["phone"] != phone:
            conn.execute("UPDATE phone_otp_tokens SET attempts = attempts + 1 WHERE id = ?", (active["id"],))
            conn.commit()
            return False
        conn.execute("UPDATE phone_otp_tokens SET used = 1 WHERE id = ?", (active["id"],))
        conn.execute("UPDATE users SET phone_verified = 1 WHERE id = ?", (user_id,))
        conn.commit()
    finally:
        conn.close()
    return True


def send_phone_otp(user_id: int, phone: str) -> Tuple[bool, str]:
    """Synchronous, unlike the broadcast alert senders -- this is a single
    user-initiated send, and the caller needs the real Twilio outcome to
    show a useful error (invalid number, unverified trial number, etc.)
    instead of always claiming success."""
    code = _generate_phone_otp(user_id, phone)
    body = f"{code} is your Aurexis verification code. Expires in {_PHONE_OTP_EXPIRE_MINUTES} minutes."
    return _twilio_send(phone, body)


# ─────────────────────────────────────────────────────────────────────────────
# HTML email templates
# ─────────────────────────────────────────────────────────────────────────────

def _new_pick_html(symbol: str, decision: str, score: float,
                   entry: Optional[float], stop: Optional[float],
                   target: Optional[float], signals: List[str],
                   first_name: str = "") -> str:
    greeting   = f"Hey {first_name}," if first_name else "Hey,"
    score_int  = int(round(score * 10)) if score <= 10 else int(round(score))
    dec_color  = "#00b450" if "HIGH" in decision else "#f0a500"
    dec_label  = decision.replace("_", " ").title()
    sigs_html  = "".join(
        f'<span style="display:inline-block;margin:3px 4px 0 0;padding:3px 10px;background:rgba(0,180,80,0.1);'
        f'border:1px solid rgba(0,180,80,0.25);border-radius:20px;font-size:11px;color:#00b450;">{s}</span>'
        for s in (signals or [])[:5]
    )
    entry_str  = f"${entry:.2f}"  if entry  else "—"
    stop_str   = f"${stop:.2f}"   if stop   else "—"
    target_str = f"${target:.2f}" if target else "—"

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#060a10;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Inter,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#060a10;padding:48px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%;">
        <tr><td style="padding:0 0 28px;text-align:center;">
          <table cellpadding="0" cellspacing="0" style="display:inline-table;">
            <tr>
              <td style="width:34px;height:34px;background:#00b450;border-radius:9px;text-align:center;vertical-align:middle;">
                <span style="font-size:17px;font-weight:900;color:#fff;line-height:34px;">A</span>
              </td>
              <td style="padding-left:9px;font-size:14px;font-weight:900;letter-spacing:0.18em;color:rgba(255,255,255,0.85);vertical-align:middle;">AUREXIS</td>
            </tr>
          </table>
        </td></tr>
        <tr><td style="background:linear-gradient(160deg,#0a1018,#0d1420);border:1px solid rgba(255,255,255,0.07);border-radius:18px;padding:40px 40px 36px;">
          <p style="margin:0 0 6px;font-size:12px;font-weight:700;letter-spacing:0.16em;text-transform:uppercase;color:{dec_color};">
            New AI Pick — {dec_label}
          </p>
          <h1 style="margin:0 0 6px;font-size:36px;font-weight:900;color:#fff;letter-spacing:-0.02em;">${symbol}</h1>
          <p style="margin:0 0 24px;font-size:14px;color:rgba(255,255,255,0.45);">AI Score: <strong style="color:#fff;">{score_int}/100</strong></p>

          <div style="margin-bottom:24px;">{sigs_html}</div>

          <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:28px;">
            <tr>
              <td style="width:33%;text-align:center;background:rgba(255,255,255,0.04);border-radius:12px;padding:16px 8px;">
                <p style="margin:0 0 4px;font-size:11px;letter-spacing:0.1em;color:rgba(255,255,255,0.4);text-transform:uppercase;">Entry</p>
                <p style="margin:0;font-size:20px;font-weight:800;color:#fff;">{entry_str}</p>
              </td>
              <td style="width:4%;"></td>
              <td style="width:30%;text-align:center;background:rgba(255,255,255,0.04);border-radius:12px;padding:16px 8px;">
                <p style="margin:0 0 4px;font-size:11px;letter-spacing:0.1em;color:rgba(255,255,255,0.4);text-transform:uppercase;">Stop</p>
                <p style="margin:0;font-size:20px;font-weight:800;color:#ef4444;">{stop_str}</p>
              </td>
              <td style="width:4%;"></td>
              <td style="width:33%;text-align:center;background:rgba(255,255,255,0.04);border-radius:12px;padding:16px 8px;">
                <p style="margin:0 0 4px;font-size:11px;letter-spacing:0.1em;color:rgba(255,255,255,0.4);text-transform:uppercase;">Target</p>
                <p style="margin:0;font-size:20px;font-weight:800;color:#00b450;">{target_str}</p>
              </td>
            </tr>
          </table>

          <a href="{_FRONTEND_URL}" style="display:block;text-align:center;background:#00b450;color:#fff;text-decoration:none;font-weight:700;font-size:15px;padding:14px 24px;border-radius:12px;letter-spacing:0.02em;">
            Open Full Analysis →
          </a>
        </td></tr>
        <tr><td style="padding:20px 0 0;text-align:center;font-size:11px;color:rgba(255,255,255,0.25);">
          You're receiving this because you enabled pick alerts in Aurexis.<br>
          <a href="{_FRONTEND_URL}/settings" style="color:rgba(255,255,255,0.35);">Manage alerts</a>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""


def _outcome_labels(status: str) -> Tuple[bool, str, str]:
    """
    Returns (is_win, email_headline_text, sms_result_text) for an outcome
    status. "won"/"lost" mean the pick actually hit its target/stop --
    honest to say so. "won_drift"/"lost_drift" mean the position expired
    after its hold window without hitting either level and closed out
    positive/negative on a 2%+ drift threshold -- a real outcome, but not
    a target hit, so it must not be described as one.
    """
    s = (status or "").lower()
    if s == "won":
        return True, "hit its target!", "HIT TARGET"
    if s == "won_drift":
        return True, "closed higher (no clean target hit)", "CLOSED UP"
    if s == "lost":
        return False, "stopped out", "Stopped out"
    if s == "lost_drift":
        return False, "closed lower (no stop hit)", "CLOSED DOWN"
    return ("won" in s), "closed", "CLOSED"


def _outcome_html(symbol: str, status: str, return_pct: Optional[float],
                  entry: Optional[float], first_name: str = "") -> str:
    greeting = f"Hey {first_name}," if first_name else "Hey,"
    is_win, headline_text, _ = _outcome_labels(status)
    color    = "#00b450" if is_win else "#ef4444"
    icon     = "✅" if is_win else "❌"
    headline = f"${symbol} {headline_text}"
    ret_str  = f"{'+' if (return_pct or 0) >= 0 else ''}{return_pct:.1f}%" if return_pct is not None else ""
    entry_str = f"${entry:.2f}" if entry else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#060a10;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Inter,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#060a10;padding:48px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%;">
        <tr><td style="padding:0 0 28px;text-align:center;">
          <table cellpadding="0" cellspacing="0" style="display:inline-table;">
            <tr>
              <td style="width:34px;height:34px;background:#00b450;border-radius:9px;text-align:center;vertical-align:middle;">
                <span style="font-size:17px;font-weight:900;color:#fff;line-height:34px;">A</span>
              </td>
              <td style="padding-left:9px;font-size:14px;font-weight:900;letter-spacing:0.18em;color:rgba(255,255,255,0.85);vertical-align:middle;">AUREXIS</td>
            </tr>
          </table>
        </td></tr>
        <tr><td style="background:linear-gradient(160deg,#0a1018,#0d1420);border:1px solid rgba(255,255,255,0.07);border-radius:18px;padding:40px 40px 36px;text-align:center;">
          <div style="font-size:40px;margin-bottom:16px;">{icon}</div>
          <p style="margin:0 0 6px;font-size:12px;font-weight:700;letter-spacing:0.16em;text-transform:uppercase;color:{color};">Pick Outcome</p>
          <h1 style="margin:0 0 8px;font-size:32px;font-weight:900;color:#fff;">{headline}</h1>
          {"<p style='margin:0 0 24px;font-size:28px;font-weight:900;color:" + color + ";'>" + ret_str + "</p>" if ret_str else ""}
          {"<p style='margin:0 0 24px;font-size:14px;color:rgba(255,255,255,0.45);'>Entry was " + entry_str + "</p>" if entry_str else ""}
          <a href="{_FRONTEND_URL}" style="display:inline-block;background:#00b450;color:#fff;text-decoration:none;font-weight:700;font-size:15px;padding:14px 32px;border-radius:12px;">
            View Dashboard →
          </a>
        </td></tr>
        <tr><td style="padding:20px 0 0;text-align:center;font-size:11px;color:rgba(255,255,255,0.25);">
          <a href="{_FRONTEND_URL}/settings" style="color:rgba(255,255,255,0.35);">Manage alerts</a>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# SMS body builders
# ─────────────────────────────────────────────────────────────────────────────

def _new_pick_sms(symbol: str, decision: str, score: float,
                  entry: Optional[float], stop: Optional[float],
                  target: Optional[float]) -> str:
    score_int = int(round(score * 10)) if score <= 10 else int(round(score))
    dec_short = "HIGH CONVICTION" if "HIGH" in decision else "LOW CONVICTION"
    parts = [f"Aurexis Pick: ${symbol} — {dec_short} (Score {score_int}/100)"]
    if entry:  parts.append(f"Entry ${entry:.2f}")
    if stop:   parts.append(f"Stop ${stop:.2f}")
    if target: parts.append(f"Target ${target:.2f}")
    parts.append(_FRONTEND_URL)
    return "\n".join(parts)


def _new_pick_sms_teaser() -> str:
    # Copy matches push.py's free-tier teaser wording, kept consistent across channels.
    return f"New pick just dropped \U0001F512 You've used your free pick for this month — upgrade to see it.\n{_FRONTEND_URL}"


def _outcome_sms(symbol: str, status: str, return_pct: Optional[float]) -> str:
    is_win, _, result = _outcome_labels(status)
    icon    = "✅" if is_win else "❌"
    ret_str = f" {'+' if (return_pct or 0) >= 0 else ''}{return_pct:.1f}%" if return_pct is not None else ""
    return f"{icon} Aurexis — ${symbol} {result}{ret_str}\n{_FRONTEND_URL}"


# ─────────────────────────────────────────────────────────────────────────────
# Public: fire new-pick alert (background)
# ─────────────────────────────────────────────────────────────────────────────

def _fire_new_pick(pick: Dict[str, Any]) -> None:
    symbol   = str(pick.get("symbol") or "").strip().upper()
    decision = str(pick.get("trade_decision") or pick.get("decision") or "").upper()
    score    = float(pick.get("final_score_0_10") or pick.get("score") or 5.0)
    tp       = pick.get("trade_plan") or {}
    targets  = tp.get("targets") or []
    entry    = tp.get("entry")  or pick.get("entry")
    stop     = tp.get("stop")   or pick.get("stop")
    target   = targets[0] if targets else tp.get("target1")
    signals  = list(pick.get("edge_signals") or [])

    try:
        entry  = float(entry)  if entry  else None
    except Exception:
        entry  = None
    try:
        stop   = float(stop)   if stop   else None
    except Exception:
        stop   = None
    try:
        target = float(target) if target else None
    except Exception:
        target = None

    if not symbol:
        return

    users = _get_opted_in_users("alerts_new_pick")
    log.info(f"alerts.new_pick: {symbol} → {len(users)} opted-in users")

    for u in users:
        channel        = str(u.get("alerts_channel") or "email").lower()
        name           = str(u.get("first_name") or "")
        email          = str(u.get("email") or "")
        phone          = str(u.get("phone") or "")
        phone_verified = bool(u.get("phone_verified"))
        plan           = str(u.get("plan") or "free").lower()
        sub_status     = str(u.get("subscription_status") or "").lower()
        is_paid        = plan in ("starter", "pro", "elite") and sub_status == "active"

        # Email stays paid-only (unchanged behavior).
        if channel in ("email", "both") and email and is_paid:
            html = _new_pick_html(symbol, decision, score, entry, stop, target, signals, name)
            _send_email(email, f"Aurexis Pick: ${symbol} — {decision.replace('_', ' ').title()}", html)

        # SMS is open to every plan (matches push.py): paid gets the real pick,
        # free gets a teaser deep-linking to upgrade. Requires a verified number.
        if channel in ("sms", "both") and phone and phone_verified:
            body = (
                _new_pick_sms(symbol, decision, score, entry, stop, target)
                if is_paid else _new_pick_sms_teaser()
            )
            _send_sms(phone, body)


def send_new_pick_alert_bg(pick: Dict[str, Any]) -> None:
    """Fire-and-forget new pick alert in a background thread."""
    threading.Thread(target=_fire_new_pick, args=(pick,), daemon=True).start()


# ─────────────────────────────────────────────────────────────────────────────
# Public: fire outcome alert (background)
# ─────────────────────────────────────────────────────────────────────────────

def _fire_outcome(symbol: str, status: str, return_pct: Optional[float],
                  entry: Optional[float]) -> None:
    if not symbol:
        return

    users = _get_opted_in_users("alerts_outcome")
    log.info(f"alerts.outcome: {symbol} {status} → {len(users)} opted-in users")

    for u in users:
        channel        = str(u.get("alerts_channel") or "email").lower()
        name           = str(u.get("first_name") or "")
        email          = str(u.get("email") or "")
        phone          = str(u.get("phone") or "")
        phone_verified = bool(u.get("phone_verified"))
        plan           = str(u.get("plan") or "free").lower()
        sub_status     = str(u.get("subscription_status") or "").lower()
        is_paid        = plan in ("starter", "pro", "elite") and sub_status == "active"

        _, headline_text, _ = _outcome_labels(status)
        ret_suffix = f" {'+' if return_pct >= 0 else ''}{return_pct:.1f}%" if return_pct is not None else ""
        subject = f"${symbol} {headline_text}{ret_suffix}"

        # Outcome alerts stay paid-only on every channel -- a free user was never
        # shown the original pick, so there's nothing to report an outcome on
        # (push.py has no free-tier teaser for outcomes either; nothing to mirror).
        if not is_paid:
            continue

        if channel in ("email", "both") and email:
            html = _outcome_html(symbol, status, return_pct, entry, name)
            _send_email(email, f"Aurexis — {subject}", html)

        if channel in ("sms", "both") and phone and phone_verified:
            body = _outcome_sms(symbol, status, return_pct)
            _send_sms(phone, body)


def send_outcome_alert_bg(symbol: str, status: str,
                          return_pct: Optional[float] = None,
                          entry: Optional[float] = None) -> None:
    """Fire-and-forget outcome alert in a background thread."""
    threading.Thread(
        target=_fire_outcome,
        args=(symbol, status, return_pct, entry),
        daemon=True,
    ).start()


# ─────────────────────────────────────────────────────────────────────────────
# Phone sanitization (E.164)
# ─────────────────────────────────────────────────────────────────────────────

def sanitize_phone(raw: Optional[str]) -> Optional[str]:
    """Normalize user input into E.164. A bare 10-digit number (no + given)
    is assumed US/Canada and gets a '1' country code prepended -- Twilio
    rejects anything else as an invalid 'To' number (error 21211)."""
    if not raw:
        return None
    raw = raw.strip()
    import re
    digits = re.sub(r"\D", "", raw)
    if raw.startswith("+"):
        cleaned = "+" + digits
    elif len(digits) == 10:
        cleaned = "+1" + digits
    elif len(digits) == 11 and digits.startswith("1"):
        cleaned = "+" + digits
    else:
        cleaned = "+" + digits
    return cleaned if len(cleaned) >= 8 else None


# ─────────────────────────────────────────────────────────────────────────────
# Public: get / save user alert preferences
# ─────────────────────────────────────────────────────────────────────────────

def get_alert_prefs(user_id: int) -> Dict[str, Any]:
    try:
        conn = sqlite3.connect(_AUTH_DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT phone, phone_verified, alerts_new_pick, alerts_outcome, alerts_channel FROM users WHERE id=?",
            (user_id,)
        ).fetchone()
        conn.close()
        if not row:
            return {"phone": None, "phone_verified": False, "alerts_new_pick": True, "alerts_outcome": True, "alerts_channel": "email"}
        return {
            "phone":           row["phone"],
            "phone_verified":  bool(row["phone_verified"]),
            "alerts_new_pick": bool(row["alerts_new_pick"]),
            "alerts_outcome":  bool(row["alerts_outcome"]),
            "alerts_channel":  row["alerts_channel"] or "email",
        }
    except Exception as e:
        log.warning(f"alerts.get_prefs: {e}")
        return {"phone": None, "phone_verified": False, "alerts_new_pick": True, "alerts_outcome": True, "alerts_channel": "email"}


def save_alert_prefs(user_id: int, phone: Optional[str],
                     alerts_new_pick: bool, alerts_outcome: bool,
                     alerts_channel: str) -> bool:
    channel = alerts_channel.lower() if alerts_channel.lower() in ("email", "sms", "both") else "email"
    phone = sanitize_phone(phone)
    try:
        conn = sqlite3.connect(_AUTH_DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        # Changing (or clearing) the phone number invalidates any prior verification --
        # a new/different number must go through send-code/verify-code again.
        current = conn.execute("SELECT phone FROM users WHERE id=?", (user_id,)).fetchone()
        phone_changed = not current or current["phone"] != phone
        if phone_changed:
            conn.execute(
                """UPDATE users
                   SET phone=?, phone_verified=0, alerts_new_pick=?, alerts_outcome=?, alerts_channel=?
                   WHERE id=?""",
                (phone, int(alerts_new_pick), int(alerts_outcome), channel, user_id)
            )
        else:
            conn.execute(
                """UPDATE users
                   SET alerts_new_pick=?, alerts_outcome=?, alerts_channel=?
                   WHERE id=?""",
                (int(alerts_new_pick), int(alerts_outcome), channel, user_id)
            )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log.warning(f"alerts.save_prefs: {e}")
        return False
