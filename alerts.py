"""
alerts.py — New-pick and outcome alerts via email (SendGrid).

Environment variables needed:
  SENDGRID_API_KEY       — already set (shared with auth.py)
  ALERT_FROM_EMAIL       — already set (shared with auth.py)
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import urllib.request as _ur
import urllib.error
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("stackiq")

_AUTH_DB_PATH   = os.getenv("AUTH_DB_PATH",  os.path.join(os.path.dirname(os.path.abspath(__file__)), "auth.db"))
_FROM_EMAIL     = os.getenv("ALERT_FROM_EMAIL", "hello@useaurexis.com")
_FRONTEND_URL   = os.getenv("FRONTEND_ORIGIN",  "https://useaurexis.com")


def _sg_key() -> str:
    return os.getenv("SENDGRID_API_KEY", "")


# ─────────────────────────────────────────────────────────────────────────────
# DB migration — add alert columns to existing users table
# ─────────────────────────────────────────────────────────────────────────────

def migrate_alerts_columns() -> None:
    """Non-destructive migration — safe to call on every startup.

    Note: `phone` and `alerts_channel` columns, the `phone_verified` column,
    and the `phone_otp_tokens` table (from a since-removed SMS alerts
    feature) are intentionally left alone here rather than dropped --
    they're unused now but harmless, and a destructive migration isn't
    worth the risk. Nothing in this module reads or writes them anymore.
    """
    try:
        conn = sqlite3.connect(_AUTH_DB_PATH, check_same_thread=False, timeout=30)
        for col, defn in [
            ("phone",            "TEXT"),
            ("alerts_new_pick",  "INTEGER NOT NULL DEFAULT 1"),
            ("alerts_outcome",   "INTEGER NOT NULL DEFAULT 1"),
            ("alerts_channel",   "TEXT NOT NULL DEFAULT 'email'"),
        ]:
            try:
                conn.execute(f"ALTER TABLE users ADD COLUMN {col} {defn}")
            except Exception:
                pass
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"alerts.migrate: {e}")


migrate_alerts_columns()


# ─────────────────────────────────────────────────────────────────────────────
# Fetch opted-in users
# ─────────────────────────────────────────────────────────────────────────────

def _get_opted_in_users(alert_col: str) -> List[Dict[str, Any]]:
    """Return active paid users who opted in to the given alert column. Free users never get alerts."""
    try:
        conn = sqlite3.connect(_AUTH_DB_PATH, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"SELECT email, first_name FROM users "
            f"WHERE {alert_col} = 1 "
            f"AND LOWER(plan) IN ('starter', 'pro') "
            f"AND LOWER(subscription_status) = 'active'"
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
        name  = str(u.get("first_name") or "")
        email = str(u.get("email") or "")
        if not email:
            continue
        html = _new_pick_html(symbol, decision, score, entry, stop, target, signals, name)
        _send_email(email, f"Aurexis Pick: ${symbol} — {decision.replace('_', ' ').title()}", html)


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
        name  = str(u.get("first_name") or "")
        email = str(u.get("email") or "")
        if not email:
            continue

        _, headline_text, _ = _outcome_labels(status)
        ret_suffix = f" {'+' if return_pct >= 0 else ''}{return_pct:.1f}%" if return_pct is not None else ""
        subject = f"${symbol} {headline_text}{ret_suffix}"

        html = _outcome_html(symbol, status, return_pct, entry, name)
        _send_email(email, f"Aurexis — {subject}", html)


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
# Public: get / save user alert preferences
# ─────────────────────────────────────────────────────────────────────────────

def get_alert_prefs(user_id: int) -> Dict[str, Any]:
    try:
        conn = sqlite3.connect(_AUTH_DB_PATH, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT alerts_new_pick, alerts_outcome FROM users WHERE id=?",
            (user_id,)
        ).fetchone()
        conn.close()
        if not row:
            return {"alerts_new_pick": True, "alerts_outcome": True}
        return {
            "alerts_new_pick": bool(row["alerts_new_pick"]),
            "alerts_outcome":  bool(row["alerts_outcome"]),
        }
    except Exception as e:
        log.warning(f"alerts.get_prefs: {e}")
        return {"alerts_new_pick": True, "alerts_outcome": True}


def save_alert_prefs(user_id: int, alerts_new_pick: bool, alerts_outcome: bool) -> bool:
    try:
        conn = sqlite3.connect(_AUTH_DB_PATH, check_same_thread=False, timeout=30)
        conn.execute(
            "UPDATE users SET alerts_new_pick=?, alerts_outcome=? WHERE id=?",
            (int(alerts_new_pick), int(alerts_outcome), user_id)
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        log.warning(f"alerts.save_prefs: {e}")
        return False
