"""
StackIQ — Auth + Stripe subscriptions module.

Registers two APIRouters:
  auth_router  → /auth/signup, /auth/login, /auth/me
  stripe_router → /stripe/create-checkout-session, /stripe/webhook

Exposes:
  get_current_user(token)          → raises 401 if invalid JWT
  require_active_subscription(user) → raises 403 if subscription not active
  JWTMiddleware                    → BaseHTTPMiddleware for path-based JWT enforcement

Required env vars:
  JWT_SECRET_KEY      — random secret (generate with `openssl rand -hex 32`)
  STRIPE_SECRET_KEY   — sk_live_… / sk_test_…
  STRIPE_WEBHOOK_SECRET — whsec_…
  STRIPE_PRICE_STARTER  — price_…
  STRIPE_PRICE_PRO      — price_…
  STRIPE_PRICE_ELITE    — price_…

Optional:
  JWT_REQUIRED_PREFIXES — comma-separated URL prefixes to protect, e.g.
                          "/api/portfolio,/api/account,/api/watchlist,/api/alerts"
                          Defaults to empty (middleware inactive; use Depends instead).
"""

from __future__ import annotations

import os
import sqlite3
import logging
import smtplib
import threading
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, EmailStr
from starlette.middleware.base import BaseHTTPMiddleware

from jose import JWTError, jwt
import bcrypt as _bcrypt_lib
import uuid as _uuid

try:
    import stripe as _stripe
except ImportError:
    _stripe = None  # type: ignore

log = logging.getLogger("stackiq.auth")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_JWT_SECRET_RAW = os.getenv("JWT_SECRET_KEY", "")
if not _JWT_SECRET_RAW or _JWT_SECRET_RAW.startswith("CHANGE_ME"):
    import sys as _sys
    _msg = (
        "FATAL: JWT_SECRET_KEY env var is not set or is using the insecure default. "
        "Generate one with: openssl rand -hex 32"
    )
    log.critical(_msg)
    # Fail hard in production; allow local dev only if DEBUG is explicitly set
    if not os.getenv("DEBUG"):
        raise RuntimeError(_msg)
    _JWT_SECRET_RAW = "dev-only-insecure-secret-do-not-use-in-prod"
JWT_SECRET = _JWT_SECRET_RAW
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_DAYS = 30

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")

PLAN_PRICES = {
    "starter": os.getenv("STRIPE_PRICE_STARTER", ""),
    "pro": os.getenv("STRIPE_PRICE_PRO", ""),
    "elite": os.getenv("STRIPE_PRICE_ELITE", ""),
}

# Reverse map: Stripe price ID → our plan string (built at import time)
_PRICE_TO_PLAN: dict[str, str] = {
    v: k for k, v in PLAN_PRICES.items() if v
}

PLAN_DISPLAY = {
    "starter": {"name": "Starter", "price_usd": 9},
    "pro": {"name": "Pro", "price_usd": 29},
    "elite": {"name": "Elite", "price_usd": 99},
}

if _stripe and STRIPE_SECRET_KEY:
    _stripe.api_key = STRIPE_SECRET_KEY

# Paths that are always exempt from JWT enforcement in JWTMiddleware
_JWT_EXEMPT = frozenset([
    "/health", "/__alive__", "/docs", "/openapi.json", "/redoc",
    "/auth/signup", "/auth/login",
    "/stripe/webhook",
])

# Comma-separated prefixes to protect, e.g. "/api/portfolio,/api/account"
_JWT_PREFIXES_RAW = os.getenv("JWT_REQUIRED_PREFIXES", "")
_JWT_PREFIXES: tuple[str, ...] = tuple(
    p.strip() for p in _JWT_PREFIXES_RAW.split(",") if p.strip()
)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

_DATA_DIR = os.getenv("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
_AUTH_DB_PATH = os.path.join(_DATA_DIR, "auth.db")


def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(_AUTH_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_auth_db() -> None:
    with _get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                email               TEXT    NOT NULL UNIQUE,
                password_hash       TEXT    NOT NULL,
                stripe_customer_id  TEXT,
                subscription_status TEXT    NOT NULL DEFAULT 'inactive',
                plan                TEXT    NOT NULL DEFAULT 'free',
                first_name          TEXT,
                last_name           TEXT,
                created_at          TEXT    NOT NULL
            )
        """)
        # Non-destructive migrations for existing databases
        for col, defn in [
            ("plan",                 "TEXT NOT NULL DEFAULT 'free'"),
            ("first_name",           "TEXT"),
            ("last_name",            "TEXT"),
            ("two_fa_enabled",       "INTEGER NOT NULL DEFAULT 0"),
            ("cancel_at_period_end", "INTEGER NOT NULL DEFAULT 0"),
            ("current_period_end",   "TEXT"),
            ("free_pick_month",      "TEXT"),
            ("session_id",           "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE users ADD COLUMN {col} {defn}")
            except Exception:
                pass  # column already exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pick_usage (
                user_id  INTEGER NOT NULL,
                date     TEXT    NOT NULL,
                count    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (user_id, date)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS otp_tokens (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER NOT NULL,
                code       TEXT    NOT NULL,
                expires_at TEXT    NOT NULL,
                used       INTEGER NOT NULL DEFAULT 0,
                attempts   INTEGER NOT NULL DEFAULT 0
            )
        """)
        try:
            conn.execute("ALTER TABLE otp_tokens ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        conn.commit()
    log.info("auth.db initialised")


def _user_plan(user: sqlite3.Row) -> str:
    """Safely read plan from a Row that may predate the plan column."""
    try:
        return str(user["plan"] or "free").lower()
    except (IndexError, KeyError):
        return "free"


# Run migration immediately on import so the plan column always exists.
init_auth_db()

# ---------------------------------------------------------------------------
# Email — welcome message sent in a background thread after signup
# ---------------------------------------------------------------------------

_SMTP_HOST     = os.getenv("SMTP_HOST", "")
_SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
_SMTP_USER     = os.getenv("SMTP_USERNAME", "")
_SMTP_PASS     = os.getenv("SMTP_PASSWORD", "")
_SMTP_TLS      = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
_FROM_EMAIL    = os.getenv("ALERT_FROM_EMAIL", "hello@useaurexis.com")
_FRONTEND_URL  = os.getenv("FRONTEND_ORIGIN", "https://useaurexis.com")

# Read dynamically so Railway env var changes take effect without a full redeploy
def _sg_key() -> str:
    return os.getenv("SENDGRID_API_KEY", "")

# Keep this for backwards compatibility with any direct reference
_SENDGRID_KEY = _sg_key()


def _welcome_html(first_name: str) -> str:
    greeting = f"Hey {first_name}," if first_name else "Hey there,"
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Welcome to Aurexis</title></head>
<body style="margin:0;padding:0;background:#060a10;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Inter,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#060a10;padding:48px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%;">

        <!-- Logo -->
        <tr><td style="padding:0 0 32px;text-align:center;">
          <table cellpadding="0" cellspacing="0" style="display:inline-table;">
            <tr>
              <td style="width:36px;height:36px;background:#00b450;border-radius:10px;text-align:center;vertical-align:middle;">
                <span style="font-size:18px;font-weight:900;color:#fff;line-height:36px;">A</span>
              </td>
              <td style="padding-left:10px;font-size:15px;font-weight:900;letter-spacing:0.18em;color:rgba(255,255,255,0.85);vertical-align:middle;">AUREXIS</td>
            </tr>
          </table>
        </td></tr>

        <!-- Card -->
        <tr><td style="background:linear-gradient(160deg,#0a1018,#0d1420);border:1px solid rgba(255,255,255,0.07);border-radius:20px;padding:48px 48px 40px;">

          <p style="margin:0 0 8px;font-size:13px;font-weight:700;letter-spacing:0.16em;text-transform:uppercase;color:#00b450;">Welcome to Aurexis</p>
          <h1 style="margin:0 0 20px;font-size:30px;font-weight:800;color:#fff;line-height:1.15;letter-spacing:-0.02em;">{greeting}<br>Your edge starts now.</h1>
          <p style="margin:0 0 32px;font-size:16px;color:rgba(255,255,255,0.50);line-height:1.75;">
            Every morning before the open, Aurexis scans 1,200+ stocks and surfaces the single highest-conviction trade setup of the day — with entry, stop-loss, and Fibonacci targets calculated for you.
          </p>

          <!-- Stats row -->
          <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:36px;">
            <tr>
              <td style="text-align:center;padding:18px;background:rgba(0,180,80,0.07);border:1px solid rgba(0,180,80,0.14);border-radius:12px;">
                <div style="font-size:28px;font-weight:900;color:#00b450;letter-spacing:-0.03em;">1,200+</div>
                <div style="font-size:11px;font-weight:600;color:rgba(255,255,255,0.30);letter-spacing:0.08em;text-transform:uppercase;margin-top:4px;">Stocks scanned</div>
              </td>
              <td width="12"></td>
              <td style="text-align:center;padding:18px;background:rgba(0,180,80,0.07);border:1px solid rgba(0,180,80,0.14);border-radius:12px;">
                <div style="font-size:28px;font-weight:900;color:#00b450;letter-spacing:-0.03em;">1</div>
                <div style="font-size:11px;font-weight:600;color:rgba(255,255,255,0.30);letter-spacing:0.08em;text-transform:uppercase;margin-top:4px;">Pick per day</div>
              </td>
              <td width="12"></td>
              <td style="text-align:center;padding:18px;background:rgba(0,180,80,0.07);border:1px solid rgba(0,180,80,0.14);border-radius:12px;">
                <div style="font-size:28px;font-weight:900;color:#00b450;letter-spacing:-0.03em;">+6.8%</div>
                <div style="font-size:11px;font-weight:600;color:rgba(255,255,255,0.30);letter-spacing:0.08em;text-transform:uppercase;margin-top:4px;">Avg winner</div>
              </td>
            </tr>
          </table>

          <!-- CTA -->
          <table width="100%" cellpadding="0" cellspacing="0">
            <tr><td align="center">
              <a href="{_FRONTEND_URL}/app" style="display:inline-block;padding:16px 40px;background:linear-gradient(135deg,#00c853,#009c3b);border-radius:12px;font-size:15px;font-weight:700;color:#fff;text-decoration:none;letter-spacing:0.3px;box-shadow:0 4px 24px rgba(0,180,80,0.35);">
                Open Aurexis →
              </a>
            </td></tr>
          </table>

        </td></tr>

        <!-- Footer -->
        <tr><td style="padding:28px 0 0;text-align:center;">
          <p style="margin:0;font-size:12px;color:rgba(255,255,255,0.18);line-height:1.6;">
            You're receiving this because you created an Aurexis account.<br>
            <a href="{_FRONTEND_URL}/privacy" style="color:rgba(255,255,255,0.25);text-decoration:underline;">Privacy Policy</a>
          </p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


def _sendgrid_send(to_email: str, subject: str, html: str, plain: str) -> bool:
    """
    Send via SendGrid. Returns True on success.
    Logs the full response body on failure so we know exactly why it failed
    (common causes: sender not verified, bad API key, plan limits).
    """
    import urllib.request as _ur
    import urllib.error  as _ue
    import json as _json

    key = _sg_key()
    if not key:
        log.warning("sendgrid: SENDGRID_API_KEY not set — cannot send email to %s", to_email)
        return False

    payload = _json.dumps({
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": _FROM_EMAIL, "name": "Aurexis"},
        "subject": subject,
        "content": [
            {"type": "text/plain", "value": plain},
            {"type": "text/html",  "value": html},
        ],
    }).encode()

    req = _ur.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=payload,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with _ur.urlopen(req, timeout=15) as resp:
            status = resp.status
        log.info("sendgrid: sent to %s (HTTP %s)", to_email, status)
        return True
    except _ue.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        log.error("sendgrid: HTTP %s sending to %s — %s", e.code, to_email, body)
        return False
    except Exception as exc:
        log.error("sendgrid: unexpected error sending to %s — %s", to_email, exc)
        return False


def _smtp_send(to_email: str, subject: str, html: str, plain: str) -> bool:
    if not _SMTP_HOST or not _SMTP_USER or not _SMTP_PASS:
        return False
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"Aurexis <{_FROM_EMAIL}>"
        msg["To"]      = to_email
        msg.attach(MIMEText(plain, "plain"))
        msg.attach(MIMEText(html,  "html"))
        with smtplib.SMTP(_SMTP_HOST, _SMTP_PORT) as s:
            if _SMTP_TLS:
                s.starttls()
            s.login(_SMTP_USER, _SMTP_PASS)
            s.sendmail(_FROM_EMAIL, to_email, msg.as_string())
        log.info("smtp: sent to %s", to_email)
        return True
    except Exception as exc:
        log.error("smtp: failed sending to %s — %s", to_email, exc)
        return False


def _send_email(to_email: str, subject: str, html: str, plain: str) -> bool:
    """Try SendGrid, fall back to SMTP. Logs clearly if both fail."""
    if _sendgrid_send(to_email, subject, html, plain):
        return True
    if _smtp_send(to_email, subject, html, plain):
        return True
    log.error("email: ALL delivery methods failed for %s (subject: %s) — "
              "set SENDGRID_API_KEY in Railway env vars", to_email, subject)
    return False


def _send_welcome_email(to_email: str, first_name: str = "") -> None:
    """Fire-and-forget welcome email. Called in a background thread."""
    subject = "Welcome to Aurexis — your edge starts now"
    html    = _welcome_html(first_name)
    plain   = (
        f"Hey {first_name or 'there'},\n\n"
        f"Welcome to Aurexis. Every morning we scan 1,200+ stocks and surface one "
        f"high-conviction trade setup with entry, stop, and targets.\n\n"
        f"Open the app: {_FRONTEND_URL}\n\nAurexis"
    )
    _send_email(to_email, subject, html, plain)


def send_welcome_email_bg(to_email: str, first_name: str = "") -> None:
    """Send welcome email in a background thread — never blocks the request."""
    threading.Thread(target=_send_welcome_email, args=(to_email, first_name), daemon=True).start()


# ---------------------------------------------------------------------------
# OTP helpers for 2FA
# ---------------------------------------------------------------------------

import secrets as _secrets

_OTP_EXPIRE_MINUTES = 10


def _generate_otp(user_id: int) -> str:
    code = f"{_secrets.randbelow(1_000_000):06d}"
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=_OTP_EXPIRE_MINUTES)).isoformat()
    with _get_db() as conn:
        # Invalidate any previous unused codes for this user
        conn.execute("UPDATE otp_tokens SET used = 1 WHERE user_id = ? AND used = 0", (user_id,))
        conn.execute(
            "INSERT INTO otp_tokens (user_id, code, expires_at) VALUES (?, ?, ?)",
            (user_id, code, expires_at),
        )
        conn.commit()
    return code


_OTP_MAX_ATTEMPTS = 10


def _verify_otp(user_id: int, code: str) -> bool:
    with _get_db() as conn:
        # Find the most recent unused, non-expired OTP for this user
        active = conn.execute(
            "SELECT id, code, expires_at, attempts FROM otp_tokens "
            "WHERE user_id = ? AND used = 0 ORDER BY id DESC LIMIT 1",
            (user_id,),
        ).fetchone()
        if not active:
            return False
        if datetime.fromisoformat(active["expires_at"]) < datetime.now(timezone.utc):
            conn.execute("UPDATE otp_tokens SET used = 1 WHERE id = ?", (active["id"],))
            conn.commit()
            return False
        attempts = int(active["attempts"] or 0)
        if attempts >= _OTP_MAX_ATTEMPTS:
            # Too many wrong guesses — invalidate this OTP
            conn.execute("UPDATE otp_tokens SET used = 1 WHERE id = ?", (active["id"],))
            conn.commit()
            return False
        if not hmac.compare_digest(str(active["code"]), str(code)):
            conn.execute("UPDATE otp_tokens SET attempts = attempts + 1 WHERE id = ?", (active["id"],))
            conn.commit()
            return False
        conn.execute("UPDATE otp_tokens SET used = 1 WHERE id = ?", (active["id"],))
        conn.commit()
    return True


def _send_otp_email(to_email: str, code: str, first_name: str = "") -> None:
    greeting = f"Hey {first_name}," if first_name else "Hey there,"
    subject = f"{code} is your Aurexis verification code"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Your Aurexis code</title></head>
<body style="margin:0;padding:0;background:#060a10;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Inter,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#060a10;padding:48px 0;">
    <tr><td align="center">
      <table width="480" cellpadding="0" cellspacing="0" style="max-width:480px;width:100%;">
        <tr><td style="padding:0 0 28px;text-align:center;">
          <table cellpadding="0" cellspacing="0" style="display:inline-table;">
            <tr>
              <td style="width:32px;height:32px;background:#00b450;border-radius:9px;text-align:center;vertical-align:middle;">
                <span style="font-size:16px;font-weight:900;color:#fff;line-height:32px;">A</span>
              </td>
              <td style="padding-left:9px;font-size:14px;font-weight:900;letter-spacing:0.18em;color:rgba(255,255,255,0.85);vertical-align:middle;">AUREXIS</span></td>
            </tr>
          </table>
        </td></tr>
        <tr><td style="background:linear-gradient(160deg,#0a1018,#0d1420);border:1px solid rgba(255,255,255,0.07);border-radius:18px;padding:40px;">
          <p style="margin:0 0 6px;font-size:11px;font-weight:700;letter-spacing:0.16em;text-transform:uppercase;color:#00b450;">Two-Factor Authentication</p>
          <h1 style="margin:0 0 16px;font-size:24px;font-weight:800;color:#fff;letter-spacing:-0.02em;">{greeting}<br>Your verification code:</h1>
          <div style="text-align:center;margin:28px 0;">
            <div style="display:inline-block;padding:20px 40px;background:rgba(0,180,80,0.08);border:1px solid rgba(0,180,80,0.20);border-radius:14px;">
              <span style="font-size:42px;font-weight:900;letter-spacing:0.18em;color:#00b450;">{code}</span>
            </div>
          </div>
          <p style="margin:0;font-size:14px;color:rgba(255,255,255,0.40);text-align:center;line-height:1.6;">
            This code expires in {_OTP_EXPIRE_MINUTES} minutes.<br>If you didn't request this, you can ignore this email.
          </p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""
    plain = f"{greeting}\n\nYour Aurexis verification code is: {code}\n\nExpires in {_OTP_EXPIRE_MINUTES} minutes."
    _send_email(to_email, subject, html, plain)


def send_otp_bg(user_id: int, to_email: str, first_name: str = "") -> str:
    code = _generate_otp(user_id)
    threading.Thread(target=_send_otp_email, args=(to_email, code, first_name), daemon=True).start()
    return code


# ---------------------------------------------------------------------------
# Password-reset OTP email
# ---------------------------------------------------------------------------

import time as _time

_pw_reset_attempts: dict[str, list[float]] = {}  # email → list of epoch timestamps
_PW_RESET_MAX = 3
_PW_RESET_WINDOW = 3600  # 1 hour

_otp_resend_attempts: dict[str, list[float]] = {}  # email → list of epoch timestamps
_OTP_RESEND_MAX = 5
_OTP_RESEND_WINDOW = 600  # 10 minutes


def _pw_reset_rate_ok(email: str) -> bool:
    now = _time.time()
    timestamps = [t for t in _pw_reset_attempts.get(email, []) if now - t < _PW_RESET_WINDOW]
    _pw_reset_attempts[email] = timestamps
    if len(timestamps) >= _PW_RESET_MAX:
        return False
    _pw_reset_attempts[email].append(now)
    return True


def _otp_resend_rate_ok(email: str) -> bool:
    now = _time.time()
    timestamps = [t for t in _otp_resend_attempts.get(email, []) if now - t < _OTP_RESEND_WINDOW]
    _otp_resend_attempts[email] = timestamps
    if len(timestamps) >= _OTP_RESEND_MAX:
        return False
    _otp_resend_attempts[email].append(now)
    return True


def _send_password_reset_email(to_email: str, code: str, first_name: str = "") -> None:
    greeting = f"Hey {first_name}," if first_name else "Hey there,"
    subject = f"{code} is your Aurexis password reset code"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Reset your Aurexis password</title></head>
<body style="margin:0;padding:0;background:#060a10;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Inter,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#060a10;padding:48px 0;">
    <tr><td align="center">
      <table width="480" cellpadding="0" cellspacing="0" style="max-width:480px;width:100%;">
        <tr><td style="padding:0 0 28px;text-align:center;">
          <table cellpadding="0" cellspacing="0" style="display:inline-table;">
            <tr>
              <td style="width:32px;height:32px;background:#00b450;border-radius:9px;text-align:center;vertical-align:middle;">
                <span style="font-size:16px;font-weight:900;color:#fff;line-height:32px;">A</span>
              </td>
              <td style="padding-left:9px;font-size:14px;font-weight:900;letter-spacing:0.18em;color:rgba(255,255,255,0.85);vertical-align:middle;">AUREXIS</td>
            </tr>
          </table>
        </td></tr>
        <tr><td style="background:linear-gradient(160deg,#0a1018,#0d1420);border:1px solid rgba(255,255,255,0.07);border-radius:18px;padding:40px;">
          <p style="margin:0 0 6px;font-size:11px;font-weight:700;letter-spacing:0.16em;text-transform:uppercase;color:#00b450;">Password Reset</p>
          <h1 style="margin:0 0 16px;font-size:24px;font-weight:800;color:#fff;letter-spacing:-0.02em;">{greeting}<br>Your reset code:</h1>
          <div style="text-align:center;margin:28px 0;">
            <div style="display:inline-block;padding:20px 40px;background:rgba(0,180,80,0.08);border:1px solid rgba(0,180,80,0.20);border-radius:14px;">
              <span style="font-size:42px;font-weight:900;letter-spacing:0.18em;color:#00b450;">{code}</span>
            </div>
          </div>
          <p style="margin:0;font-size:14px;color:rgba(255,255,255,0.40);text-align:center;line-height:1.6;">
            This code expires in {_OTP_EXPIRE_MINUTES} minutes.<br>If you didn't request this, you can safely ignore this email.
          </p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""
    plain = f"{greeting}\n\nYour Aurexis password reset code is: {code}\n\nExpires in {_OTP_EXPIRE_MINUTES} minutes.\nIf you didn't request this, ignore this email."
    _send_email(to_email, subject, html, plain)


# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------

def hash_password(plain: str) -> str:
    return _bcrypt_lib.hashpw(plain.encode("utf-8"), _bcrypt_lib.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return _bcrypt_lib.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------

PLAN_RANK = {"free": 0, "starter": 1, "pro": 2, "elite": 3}


def _new_session(user_id: int) -> str:
    """Generate a fresh session_id, persist it, and return it."""
    sid = str(_uuid.uuid4())
    with _get_db() as conn:
        conn.execute("UPDATE users SET session_id = ? WHERE id = ?", (sid, user_id))
        conn.commit()
    return sid


def create_access_token(user_id: int, email: str, plan: str = "free", session_id: str = "") -> str:
    expire = datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS)
    payload = {"sub": str(user_id), "email": email, "plan": plan, "exp": expire}
    if session_id:
        payload["sid"] = session_id
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    """Decode and validate a JWT. Raises JWTError on failure."""
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])


# ---------------------------------------------------------------------------
# FastAPI security dependency
# ---------------------------------------------------------------------------

_bearer = HTTPBearer(auto_error=False)


def _extract_token(
    request: Request,
    creds: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
    authorization: Optional[str] = Header(default=None),
) -> str:
    """Pull token from Bearer header or httpOnly cookie. Raises 401 if absent."""
    token: Optional[str] = None
    if creds:
        token = creds.credentials
    elif authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
    if not token:
        # Fall back to the httpOnly session cookie
        token = request.cookies.get("sq_token") or None
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return token


def get_current_user(token: str = Depends(_extract_token)) -> sqlite3.Row:
    """Dependency: decode JWT and return the user row. Raises 401 on failure."""
    credentials_exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = decode_token(token)
        user_id = int(payload["sub"])
    except (JWTError, KeyError, ValueError):
        raise credentials_exc

    with _get_db() as conn:
        user = conn.execute(
            "SELECT * FROM users WHERE id = ?", (user_id,)
        ).fetchone()
    if user is None:
        raise credentials_exc

    # Single-session enforcement: if the DB has a session_id set, the JWT must match.
    # Users without session_id in DB (logged in before this feature) are allowed through
    # until their next login generates one.
    try:
        db_sid = user["session_id"]
    except (IndexError, KeyError):
        db_sid = None
    jwt_sid = payload.get("sid")
    if db_sid and jwt_sid != db_sid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expired, please log in again",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user


def require_active_subscription(user: sqlite3.Row = Depends(get_current_user)) -> sqlite3.Row:
    """Dependency: like get_current_user but also checks subscription_status."""
    if user["subscription_status"] != "active":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Active subscription required",
        )
    return user


def require_plan(min_plan: str):
    """
    Dependency factory for plan-level gating.

    Usage:
        @app.get("/foo")
        def foo(_user = Depends(require_plan("starter"))):
            ...
    """
    def _dep(user: sqlite3.Row = Depends(get_current_user)) -> sqlite3.Row:
        user_plan = _user_plan(user)
        if PLAN_RANK.get(user_plan, 0) < PLAN_RANK.get(min_plan, 0):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"PLAN_UPGRADE_REQUIRED:{min_plan}",
            )
        return user
    return _dep


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class SignupRequest(BaseModel):
    email: EmailStr
    password: str
    first_name: str = ""
    last_name: str = ""


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class CheckoutRequest(BaseModel):
    plan: str
    success_url: str
    cancel_url: str

    def validated_urls(self) -> tuple[str, str]:
        """Return (success_url, cancel_url) after origin validation. Raises 400 on bad input."""
        _allowed_origins_raw = os.getenv("ALLOWED_ORIGINS", "")
        _allowed = {o.strip().rstrip("/") for o in _allowed_origins_raw.split(",") if o.strip()}
        # Always allow localhost for dev
        _allowed.update({"http://localhost:3000", "http://localhost:5173", "http://localhost:8000"})

        def _check(url: str) -> str:
            from urllib.parse import urlparse
            p = urlparse(url)
            if p.scheme not in ("http", "https"):
                raise HTTPException(400, "INVALID_REDIRECT_URL")
            origin = f"{p.scheme}://{p.netloc}"
            if _allowed and origin not in _allowed:
                raise HTTPException(400, "INVALID_REDIRECT_URL")
            return url

        return _check(self.success_url), _check(self.cancel_url)


# ---------------------------------------------------------------------------
# Auth router
# ---------------------------------------------------------------------------

auth_router = APIRouter(prefix="/auth", tags=["auth"])

_IS_PROD = not bool(os.getenv("DEBUG"))
_COOKIE_MAX_AGE = JWT_EXPIRE_DAYS * 86400


def _set_auth_cookie(response: Response, token: str) -> None:
    """Attach an httpOnly session cookie alongside the JSON body token."""
    response.set_cookie(
        key="sq_token",
        value=token,
        max_age=_COOKIE_MAX_AGE,
        httponly=True,
        secure=_IS_PROD,        # Secure=True in prod (HTTPS); False in local dev
        samesite="lax",         # lax allows top-level navigation redirects (e.g. Stripe return)
        path="/",
    )


@auth_router.post("/signup", status_code=201)
def signup(body: SignupRequest, response: Response):
    if len(body.password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")

    pw_hash = hash_password(body.password)
    now = datetime.now(timezone.utc).isoformat()
    first = body.first_name.strip()[:64]
    last = body.last_name.strip()[:64]
    try:
        with _get_db() as conn:
            cur = conn.execute(
                "INSERT INTO users (email, password_hash, first_name, last_name, created_at) VALUES (?, ?, ?, ?, ?)",
                (body.email, pw_hash, first or None, last or None, now),
            )
            conn.commit()
            user_id = cur.lastrowid
    except sqlite3.IntegrityError:
        raise HTTPException(409, "Email already registered")

    # Send OTP for mandatory 2FA — welcome email fires after OTP verified
    send_otp_bg(user_id, body.email, first)
    return {"requires_2fa": True, "email": body.email, "first_name": first, "is_new_user": True}


@auth_router.post("/login")
def login(body: LoginRequest, response: Response):
    with _get_db() as conn:
        user = conn.execute(
            "SELECT * FROM users WHERE LOWER(email) = ?", (body.email.lower(),)
        ).fetchone()

    if user is None or not verify_password(body.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    # 2FA is mandatory — always send OTP and require verification
    first = ""
    try: first = user["first_name"] or ""
    except (IndexError, KeyError): pass
    send_otp_bg(user["id"], user["email"], first)
    return {"requires_2fa": True, "email": user["email"]}


class OTPRequest(BaseModel):
    email: EmailStr
    code: str


class OTPVerifyRequest(BaseModel):
    email: EmailStr
    code: str
    is_new_user: bool = False


@auth_router.post("/verify-otp")
def verify_otp(body: OTPVerifyRequest, response: Response):
    with _get_db() as conn:
        user = conn.execute("SELECT * FROM users WHERE LOWER(email) = ?", (body.email.lower(),)).fetchone()
    if user is None:
        raise HTTPException(401, "Invalid code")
    if not _verify_otp(user["id"], body.code.strip()):
        raise HTTPException(401, "Invalid or expired code")
    plan = _user_plan(user)
    sid = _new_session(user["id"])
    token = create_access_token(user["id"], user["email"], plan=plan, session_id=sid)
    _set_auth_cookie(response, token)
    if body.is_new_user:
        first = ""
        try: first = user["first_name"] or ""
        except (IndexError, KeyError): pass
        send_welcome_email_bg(user["email"], first)
    return {
        "access_token": token,
        "token_type": "bearer",
        "user_id": user["id"],
        "email": user["email"],
        "plan": plan,
        "first_name": user["first_name"] if "first_name" in user.keys() else "",
    }


@auth_router.post("/resend-otp")
def resend_otp(body: OTPRequest):
    """Resend OTP to an existing user (used by signup OTP screen resend button)."""
    email = (body.email or "").strip().lower()
    if not _otp_resend_rate_ok(email):
        return {"ok": True}  # Silently drop — don't reveal rate limiting
    with _get_db() as conn:
        user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    if user is None:
        return {"ok": True}  # Don't reveal whether email exists
    first = ""
    try: first = user["first_name"] or ""
    except (IndexError, KeyError): pass
    send_otp_bg(user["id"], user["email"], first)
    return {"ok": True}


class Enable2FARequest(BaseModel):
    code: str


@auth_router.post("/2fa/send-setup-code")
def send_setup_code(user: sqlite3.Row = Depends(get_current_user)):
    first = ""
    try: first = user["first_name"] or ""
    except (IndexError, KeyError): pass
    send_otp_bg(user["id"], user["email"], first)
    return {"ok": True}


@auth_router.post("/2fa/enable")
def enable_2fa(body: Enable2FARequest, user: sqlite3.Row = Depends(get_current_user)):
    if not _verify_otp(user["id"], body.code.strip()):
        raise HTTPException(400, "Invalid or expired code")
    with _get_db() as conn:
        conn.execute("UPDATE users SET two_fa_enabled = 1 WHERE id = ?", (user["id"],))
        conn.commit()
    return {"ok": True, "two_fa_enabled": True}


@auth_router.post("/2fa/disable")
def disable_2fa(user: sqlite3.Row = Depends(get_current_user)):
    with _get_db() as conn:
        conn.execute("UPDATE users SET two_fa_enabled = 0 WHERE id = ?", (user["id"],))
        conn.commit()
    return {"ok": True, "two_fa_enabled": False}


@auth_router.post("/logout")
def logout(response: Response):
    """Clear the httpOnly session cookie."""
    response.delete_cookie(key="sq_token", path="/", httponly=True, secure=_IS_PROD, samesite="lax")
    return {"ok": True}


@auth_router.get("/me")
def me(user: sqlite3.Row = Depends(get_current_user)):
    from datetime import datetime, timezone, timedelta
    def _safe(col):
        try: return user[col]
        except (IndexError, KeyError): return None

    plan = _user_plan(user)
    picks_this_week = None
    picks_limit = None
    if plan == "starter":
        today = datetime.now(timezone.utc).date()
        week_start = today - timedelta(days=today.weekday())
        with _get_db() as _c:
            row = _c.execute(
                "SELECT count FROM pick_usage WHERE user_id = ? AND date = ?",
                (user["id"], week_start.isoformat()),
            ).fetchone()
        picks_this_week = int(row["count"]) if row else 0
        picks_limit = 3

    return {
        "id": user["id"],
        "email": user["email"],
        "plan": plan,
        "subscription_status": user["subscription_status"],
        "stripe_customer_id": user["stripe_customer_id"],
        "created_at": user["created_at"],
        "first_name": _safe("first_name"),
        "last_name": _safe("last_name"),
        "two_fa_enabled": bool(_safe("two_fa_enabled")),
        "cancel_at_period_end": bool(_safe("cancel_at_period_end")),
        "current_period_end": _safe("current_period_end"),
        "picks_this_week": picks_this_week,
        "picks_limit": picks_limit,
    }



@auth_router.post("/refresh-token")
def refresh_token(response: Response, user: sqlite3.Row = Depends(get_current_user)):
    """Issue a new JWT reflecting the current plan/status from DB."""
    plan = _user_plan(user)
    try:
        sid = user["session_id"] or ""
    except (IndexError, KeyError):
        sid = ""
    token = create_access_token(user["id"], user["email"], plan=plan, session_id=sid)
    _set_auth_cookie(response, token)
    return {
        "access_token": token,
        "token_type": "bearer",
        "plan": plan,
        "subscription_status": user["subscription_status"],
    }


@auth_router.post("/test-email")
def test_email(user: sqlite3.Row = Depends(get_current_user)):
    """
    Send a test email to the authenticated user's address.
    Use this to verify SendGrid is configured correctly on Railway.
    """
    to = user["email"]
    name = ""
    try: name = user["first_name"] or ""
    except Exception: pass

    key = _sg_key()
    delivered = _send_email(
        to_email = to,
        subject  = "Aurexis — email delivery test",
        html     = f"<p style='font-family:sans-serif'>Hey {name or 'there'},<br><br>This is a test email from Aurexis. If you see this, email delivery is working.</p>",
        plain    = f"Hey {name or 'there'}, this is a test email from Aurexis. Email delivery is working.",
    )
    return {
        "ok": delivered,
        "to": to,
        "sendgrid_key_set": bool(key),
        "from_email": _FROM_EMAIL,
        "smtp_configured": bool(_SMTP_HOST and _SMTP_USER),
    }


@auth_router.post("/test-welcome")
def test_welcome_email(user: sqlite3.Row = Depends(get_current_user)):
    """Re-fire the welcome email to the current user. For testing only."""
    to = user["email"]
    name = ""
    try: name = user["first_name"] or ""
    except Exception: pass
    delivered = _send_email(
        to_email=to,
        subject="Welcome to Aurexis — your edge starts now",
        html=_welcome_html(name),
        plain=(
            f"Hey {name or 'there'},\n\nWelcome to Aurexis. Every morning we scan 1,200+ stocks "
            f"and surface one high-conviction trade setup with entry, stop, and targets.\n\n"
            f"Open the app: {_FRONTEND_URL}\n\nAurexis"
        ),
    )
    return {"ok": delivered, "to": to, "from_email": _FROM_EMAIL}


@auth_router.post("/test-alert")
def test_alert_email(user: sqlite3.Row = Depends(get_current_user)):
    """Fire a test new-pick alert email to the current user."""
    from alerts import _new_pick_html, _send_email as _alerts_send_email
    to = user["email"]
    name = ""
    try: name = user["first_name"] or ""
    except Exception: pass
    html = _new_pick_html(
        symbol="NVDA", decision="HIGH_CONVICTION", score=8.4,
        entry=875.20, stop=852.00, target=920.00,
        signals=["MOMENTUM_EXPANSION", "BREAKOUT_STRUCTURE", "RS_LEADER"],
        first_name=name,
    )
    delivered = _alerts_send_email(to, "New Aurexis AI Pick — NVDA (test)", html)
    return {"ok": delivered, "to": to}


@auth_router.delete("/delete-account")
def delete_account(response: Response, user: sqlite3.Row = Depends(get_current_user)):
    with _get_db() as conn:
        conn.execute("DELETE FROM users WHERE id = ?", (user["id"],))
        conn.commit()
    response.delete_cookie(key="sq_token", path="/", httponly=True, secure=_IS_PROD, samesite="lax")
    return {"ok": True}


class ForgotPasswordRequest(BaseModel):
    email: str


class ResetPasswordRequest(BaseModel):
    email: str
    code: str
    new_password: str


@auth_router.post("/forgot-password")
def forgot_password(body: ForgotPasswordRequest):
    """Send a password-reset OTP. Always returns ok=true to prevent account enumeration."""
    email = (body.email or "").strip().lower()
    if not email:
        return {"ok": True}

    if not _pw_reset_rate_ok(email):
        # Still return ok to avoid enumeration, just don't send
        return {"ok": True}

    with _get_db() as conn:
        row = conn.execute(
            "SELECT id, first_name FROM users WHERE LOWER(email) = ?", (email,)
        ).fetchone()

    if row:
        code = _generate_otp(row["id"])
        first_name = ""
        try:
            first_name = row["first_name"] or ""
        except Exception:
            pass
        threading.Thread(
            target=_send_password_reset_email,
            args=(email, code, first_name),
            daemon=True,
        ).start()

    return {"ok": True}


@auth_router.post("/reset-password")
def reset_password(body: ResetPasswordRequest):
    """Verify OTP and set a new password. Does NOT issue a token — user must log in fresh."""
    email = (body.email or "").strip().lower()
    code = (body.code or "").strip()
    new_password = body.new_password or ""

    if len(new_password) < 8:
        raise HTTPException(status_code=422, detail="Password must be at least 8 characters.")

    with _get_db() as conn:
        row = conn.execute(
            "SELECT id FROM users WHERE LOWER(email) = ?", (email,)
        ).fetchone()

    if not row:
        raise HTTPException(status_code=400, detail="Invalid or expired reset code.")

    if not _verify_otp(row["id"], code):
        raise HTTPException(status_code=400, detail="Invalid or expired reset code.")

    new_hash = hash_password(new_password)
    with _get_db() as conn:
        conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (new_hash, row["id"]),
        )
        conn.commit()

    return {"ok": True}


_ADMIN_SECRET = os.getenv("ADMIN_SECRET", "aurexis_admin_2026")
_VALID_PLANS = {"free", "starter", "pro", "elite"}


class AdminGetTokenRequest(BaseModel):
    secret: str
    email: str


@auth_router.post("/admin-get-token")
def admin_get_token(body: AdminGetTokenRequest):
    """Dev-only: issue a JWT for any account using admin secret. For curl testing."""
    if body.secret != _ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")
    email = body.email.strip().lower()
    with _get_db() as conn:
        user = conn.execute("SELECT * FROM users WHERE LOWER(email) = ?", (email,)).fetchone()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    plan = _user_plan(user)
    try:
        sid = user["session_id"] or ""
    except (IndexError, KeyError):
        sid = ""
    token = create_access_token(user["id"], user["email"], plan=plan, session_id=sid)
    return {"access_token": token, "plan": plan, "email": email}


class AdminStripeLookupRequest(BaseModel):
    secret: str
    customer_id: Optional[str] = None
    email: Optional[str] = None
    subscription_id: Optional[str] = None


@auth_router.post("/admin-stripe-lookup")
def admin_stripe_lookup(body: AdminStripeLookupRequest):
    """
    Diagnostic: does our configured STRIPE_SECRET_KEY see a given customer
    (or all customers under an email) at all, and what subscriptions +
    our own DB row do they resolve to? Read-only.
    """
    if body.secret != _ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")
    if _stripe is None or not STRIPE_SECRET_KEY:
        raise HTTPException(503, "Stripe not configured")

    key_mode = "live" if STRIPE_SECRET_KEY.startswith("sk_live_") else (
        "test" if STRIPE_SECRET_KEY.startswith("sk_test_") else "unknown"
    )

    def _customer_report(customer_id: str) -> dict:
        entry = {"customer_id": customer_id}
        try:
            cust = _stripe_to_dict(_stripe.Customer.retrieve(customer_id))
            entry["customer_found"] = True
            entry["customer_email"] = cust.get("email")
            entry["customer_livemode"] = cust.get("livemode")
            entry["customer_created"] = cust.get("created")
        except Exception as exc:
            entry["customer_found"] = False
            entry["error"] = str(exc)
            return entry

        try:
            subs = _stripe_to_dict(_stripe.Subscription.list(customer=customer_id, limit=10))
            entry["subscriptions"] = [
                {"id": s.get("id"), "status": s.get("status"), "livemode": s.get("livemode")}
                for s in (subs.get("data") or [])
            ]
        except Exception as exc:
            entry["subscriptions_error"] = str(exc)

        with _get_db() as conn:
            row = conn.execute(
                "SELECT id, email, plan, subscription_status FROM users WHERE stripe_customer_id = ?",
                (customer_id,),
            ).fetchone()
        entry["matches_db_user"] = dict(row) if row else None
        return entry

    result = {"configured_key_mode": key_mode}

    if body.customer_id:
        result["by_customer_id"] = _customer_report(body.customer_id)

    if body.email:
        try:
            cust_list = _stripe_to_dict(_stripe.Customer.list(email=body.email, limit=20))
            customers = cust_list.get("data") or []
            result["by_email"] = {
                "email_searched": body.email,
                "customers_found": len(customers),
                "customers": [_customer_report(_stripe_to_dict(c).get("id")) for c in customers],
            }
        except Exception as exc:
            result["by_email_error"] = str(exc)

        # Case-insensitive DB lookup, showing the *raw* stored email so any
        # whitespace/typo/case mismatch against the searched address is visible.
        try:
            with _get_db() as conn:
                db_rows = conn.execute(
                    "SELECT id, email, plan, subscription_status, stripe_customer_id, created_at "
                    "FROM users WHERE LOWER(TRIM(email)) = LOWER(TRIM(?))",
                    (body.email,),
                ).fetchall()
            result["db_users_matching_email_ci"] = [dict(r) for r in db_rows]
        except Exception as exc:
            result["db_lookup_error"] = str(exc)

    if body.subscription_id:
        try:
            sub = _stripe_to_dict(_stripe.Subscription.retrieve(body.subscription_id))
            cust_id = sub.get("customer")
            with _get_db() as conn:
                row = conn.execute(
                    "SELECT id, email, plan, subscription_status, stripe_customer_id FROM users WHERE stripe_customer_id = ?",
                    (cust_id,),
                ).fetchone()
            result["by_subscription_id"] = {
                "subscription_id": body.subscription_id,
                "found": True,
                "status": sub.get("status"),
                "livemode": sub.get("livemode"),
                "customer": cust_id,
                "metadata": sub.get("metadata"),
                "expected_plan": _plan_from_stripe_sub(sub),
                "current_period_end": sub.get("current_period_end"),
                "matches_db_user": dict(row) if row else None,
            }
        except Exception as exc:
            result["by_subscription_id"] = {"subscription_id": body.subscription_id, "found": False, "error": str(exc)}

    return result


class AdminReconcileStripeRequest(BaseModel):
    secret: str


@auth_router.post("/admin-reconcile-stripe")
def admin_reconcile_stripe(body: AdminReconcileStripeRequest):
    """
    Cross-check every active Stripe subscription against what our DB thinks
    that customer's plan/status is. Read-only — makes no writes.

    Surfaces two failure modes:
      - "mismatches": we know the customer (stripe_customer_id or
        metadata.user_id resolved to a user row) but their DB plan/status
        doesn't match what Stripe says they're actively paying for.
      - "orphans": Stripe has an active subscription for a customer we can't
        resolve to any user row at all (stripe_customer_id was never saved,
        and/or metadata.user_id is missing/stale).
    """
    if body.secret != _ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")
    if _stripe is None or not STRIPE_SECRET_KEY:
        raise HTTPException(503, "Stripe not configured")

    mismatches: List[dict] = []
    orphans: List[dict] = []
    checked = 0
    starting_after: Optional[str] = None

    while True:
        kwargs = {"status": "active", "limit": 100}
        if starting_after:
            kwargs["starting_after"] = starting_after
        page = _stripe_to_dict(_stripe.Subscription.list(**kwargs))
        subs = page.get("data", [])
        if not subs:
            break

        for sub in subs:
            checked += 1
            d = _stripe_to_dict(sub)
            customer_id = d.get("customer")
            sub_id = d.get("id")
            expected_plan = _plan_from_stripe_sub(d)
            meta_uid = (d.get("metadata") or {}).get("user_id")

            row = None
            with _get_db() as conn:
                if customer_id:
                    row = conn.execute(
                        "SELECT id, email, plan, subscription_status FROM users WHERE stripe_customer_id = ?",
                        (customer_id,),
                    ).fetchone()
                if not row and meta_uid:
                    try:
                        row = conn.execute(
                            "SELECT id, email, plan, subscription_status FROM users WHERE id = ?",
                            (int(meta_uid),),
                        ).fetchone()
                    except ValueError:
                        pass

            if not row:
                cust_email = None
                try:
                    cust = _stripe.Customer.retrieve(customer_id)
                    cust_email = cust.get("email")
                except Exception:
                    pass
                orphans.append({
                    "stripe_customer_id": customer_id,
                    "stripe_subscription_id": sub_id,
                    "stripe_email": cust_email,
                    "expected_plan": expected_plan,
                    "reason": "no user row matched stripe_customer_id or metadata.user_id",
                })
                continue

            db_plan = row["plan"]
            db_status = row["subscription_status"]
            if db_plan != expected_plan or db_status != "active":
                mismatches.append({
                    "user_id": row["id"],
                    "email": row["email"],
                    "stripe_customer_id": customer_id,
                    "stripe_subscription_id": sub_id,
                    "expected_plan": expected_plan,
                    "db_plan": db_plan,
                    "db_subscription_status": db_status,
                })

        if not page.get("has_more"):
            break
        starting_after = subs[-1]["id"]

    return {
        "checked_active_subscriptions": checked,
        "mismatches_found": len(mismatches),
        "orphans_found": len(orphans),
        "mismatches": mismatches,
        "orphans": orphans,
    }


class AdminResetPicksRequest(BaseModel):
    secret: str
    email: str


@auth_router.post("/admin-reset-picks")
def admin_reset_picks(body: AdminResetPicksRequest):
    """Dev-only: clear pick_usage rows for a user so weekly limit resets."""
    if body.secret != _ADMIN_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")
    with _get_db() as conn:
        row = conn.execute("SELECT id FROM users WHERE LOWER(email) = ?", (body.email.strip().lower(),)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User not found")
        conn.execute("DELETE FROM pick_usage WHERE user_id = ?", (row["id"],))
        conn.commit()
    return {"ok": True, "email": body.email.strip().lower()}



def _owner_upgrade() -> None:
    try:
        with _get_db() as conn:
            for email in ("ishanbais13@gmail.com", "baisishan48@gmail.com"):
                conn.execute(
                    "UPDATE users SET plan='elite', subscription_status='active' WHERE email=?",
                    (email,),
                )
            conn.commit()
    except Exception as exc:
        log.warning("_owner_upgrade failed: %s", exc)

_owner_upgrade()


# ---------------------------------------------------------------------------
# Stripe router
# ---------------------------------------------------------------------------

stripe_router = APIRouter(prefix="/stripe", tags=["stripe"])


def _ensure_stripe():
    if _stripe is None:
        raise HTTPException(503, "Stripe SDK not installed")
    if not STRIPE_SECRET_KEY:
        raise HTTPException(503, "STRIPE_SECRET_KEY not configured")


@stripe_router.post("/create-checkout-session")
def create_checkout_session(
    body: CheckoutRequest,
    user: sqlite3.Row = Depends(get_current_user),
):
    _ensure_stripe()

    plan_key = body.plan.lower()
    if plan_key not in PLAN_PRICES:
        raise HTTPException(400, f"Unknown plan '{body.plan}'. Choose: starter, pro, elite")
    price_id = PLAN_PRICES[plan_key]
    if not price_id:
        raise HTTPException(503, f"STRIPE_PRICE_{plan_key.upper()} not configured")

    # Reuse or create a Stripe customer
    customer_id: Optional[str] = user["stripe_customer_id"]
    if not customer_id:
        customer = _stripe.Customer.create(email=user["email"])
        customer_id = customer["id"]
        with _get_db() as conn:
            conn.execute(
                "UPDATE users SET stripe_customer_id = ? WHERE id = ?",
                (customer_id, user["id"]),
            )
            conn.commit()

    success_url, cancel_url = body.validated_urls()
    session = _stripe.checkout.Session.create(
        customer=customer_id,
        payment_method_types=["card"],
        mode="subscription",
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={"user_id": str(user["id"])},
        subscription_data={"metadata": {"user_id": str(user["id"])}},
    )
    return {"checkout_url": session["url"], "session_id": session["id"]}


@stripe_router.post("/cancel-subscription")
def cancel_subscription(user: sqlite3.Row = Depends(get_current_user)):
    _ensure_stripe()
    customer_id = user["stripe_customer_id"]
    if not customer_id:
        raise HTTPException(400, "No Stripe customer on file")
    subs = _stripe.Subscription.list(customer=customer_id, status="active", limit=1)
    if not subs.data:
        raise HTTPException(404, "No active subscription found")
    sub = _stripe.Subscription.modify(subs.data[0].id, cancel_at_period_end=True)
    _update_subscription_from_stripe(sub)
    raw_end = sub.get("current_period_end")
    period_end_iso = (
        datetime.fromtimestamp(raw_end, tz=timezone.utc).isoformat() if raw_end else None
    )
    log.info("cancel_subscription: user_id=%s cancel_at_period_end=True end=%s", user["id"], period_end_iso)
    return {"cancel_at_period_end": True, "current_period_end": period_end_iso}


@stripe_router.post("/reactivate-subscription")
def reactivate_subscription(user: sqlite3.Row = Depends(get_current_user)):
    _ensure_stripe()
    customer_id = user["stripe_customer_id"]
    if not customer_id:
        raise HTTPException(400, "No Stripe customer on file")
    subs = _stripe.Subscription.list(customer=customer_id, limit=1)
    if not subs.data:
        raise HTTPException(404, "No subscription found")
    sub = _stripe.Subscription.modify(subs.data[0].id, cancel_at_period_end=False)
    _update_subscription_from_stripe(sub)
    log.info("reactivate_subscription: user_id=%s", user["id"])
    return {"cancel_at_period_end": False}


@stripe_router.get("/payment-method")
def get_payment_method(user: sqlite3.Row = Depends(get_current_user)):
    _ensure_stripe()
    customer_id = user["stripe_customer_id"]
    if not customer_id:
        return {"payment_method": None}
    try:
        # Try attached payment methods first (works for both old and new Stripe flows)
        pms = _stripe.PaymentMethod.list(customer=customer_id, type="card", limit=1)
        if pms.data:
            pm = pms.data[0]
            card = pm.card
            return {"payment_method": {
                "brand": card.brand.title(),
                "last4": card.last4,
                "exp_month": card.exp_month,
                "exp_year": card.exp_year,
            }}
        # Fallback: check default_source (legacy card attach)
        customer = _stripe.Customer.retrieve(customer_id, expand=["default_source"])
        src = customer.get("default_source")
        if src and isinstance(src, dict) and src.get("object") == "card":
            return {"payment_method": {
                "brand": src.get("brand", "Card").title(),
                "last4": src.get("last4", ""),
                "exp_month": src.get("exp_month"),
                "exp_year": src.get("exp_year"),
            }}
        return {"payment_method": None}
    except Exception as e:
        log.warning("get_payment_method error: %s", e)
        return {"payment_method": None}


@stripe_router.get("/invoices")
def get_invoices(user: sqlite3.Row = Depends(get_current_user)):
    _ensure_stripe()
    customer_id = user["stripe_customer_id"]
    if not customer_id:
        return {"invoices": []}
    try:
        inv_list = _stripe.Invoice.list(customer=customer_id, limit=12)
        invoices = []
        for inv in inv_list.data:
            invoices.append({
                "id": inv["id"],
                "date": datetime.fromtimestamp(inv["created"], tz=timezone.utc).strftime("%b %d, %Y"),
                "amount": f"${inv['amount_paid'] / 100:.2f}",
                "status": inv["status"].title(),
                "url": inv.get("hosted_invoice_url") or inv.get("invoice_pdf"),
            })
        return {"invoices": invoices}
    except Exception:
        return {"invoices": []}


@stripe_router.post("/billing-portal")
def billing_portal(user: sqlite3.Row = Depends(get_current_user)):
    _ensure_stripe()
    customer_id = user["stripe_customer_id"]
    if not customer_id:
        raise HTTPException(400, "No Stripe customer on file")
    session = _stripe.billing_portal.Session.create(
        customer=customer_id,
        return_url="https://useaurexis.com/app",
    )
    return {"url": session["url"]}


@stripe_router.get("/plans")
def list_plans():
    """Public endpoint — returns plan info without prices IDs."""
    return {
        plan: {**info, "price_configured": bool(PLAN_PRICES[plan])}
        for plan, info in PLAN_DISPLAY.items()
    }


# Maps Stripe subscription status strings → our internal status
_SUB_STATUS_MAP = {
    "active": "active",
    "trialing": "active",
    "past_due": "past_due",
    "unpaid": "past_due",
    "canceled": "cancelled",
    "incomplete": "inactive",
    "incomplete_expired": "inactive",
    "paused": "inactive",
}


def _stripe_to_dict(obj) -> dict:
    """Safely convert a Stripe SDK object (StripeObject) or plain dict to a regular dict."""
    if isinstance(obj, dict):
        return obj
    try:
        return obj.to_dict_recursive()
    except AttributeError:
        pass
    try:
        return dict(obj)
    except Exception:
        return {}


def _plan_from_stripe_sub(stripe_sub) -> str:
    """Extract our plan string from a Stripe subscription object."""
    try:
        d = _stripe_to_dict(stripe_sub)
        items_data = d.get("items", {})
        if isinstance(items_data, dict):
            items = items_data.get("data", [])
        else:
            items = []
        for item in items:
            item_d = _stripe_to_dict(item) if not isinstance(item, dict) else item
            price = item_d.get("price", {})
            price_d = _stripe_to_dict(price) if not isinstance(price, dict) else price
            price_id = price_d.get("id", "")
            if price_id in _PRICE_TO_PLAN:
                return _PRICE_TO_PLAN[price_id]
    except Exception:
        pass
    return "free"


def _update_subscription_from_stripe(stripe_sub) -> None:
    """Persist subscription status and plan change to the users table."""
    d = _stripe_to_dict(stripe_sub)
    raw_status = d.get("status", "inactive")
    new_status = _SUB_STATUS_MAP.get(raw_status, "inactive")
    new_plan = _plan_from_stripe_sub(d) if new_status == "active" else "free"

    # Prefer metadata user_id, fall back to customer lookup
    user_id: Optional[int] = None
    meta_uid = (d.get("metadata") or {}).get("user_id")
    if meta_uid:
        try:
            user_id = int(meta_uid)
        except ValueError:
            pass

    if user_id is None:
        customer_id = d.get("customer")
        if customer_id:
            with _get_db() as conn:
                row = conn.execute(
                    "SELECT id FROM users WHERE stripe_customer_id = ?", (customer_id,)
                ).fetchone()
                if row:
                    user_id = row["id"]

    if user_id is None:
        log.warning("stripe webhook: could not resolve user for subscription %s", d.get("id"))
        return

    cancel_at_end = int(bool(d.get("cancel_at_period_end", False)))
    raw_period_end = d.get("current_period_end")
    period_end_iso = (
        datetime.fromtimestamp(raw_period_end, tz=timezone.utc).isoformat()
        if raw_period_end else None
    )

    with _get_db() as conn:
        conn.execute(
            "UPDATE users SET subscription_status = ?, plan = ?, cancel_at_period_end = ?, current_period_end = ? WHERE id = ?",
            (new_status, new_plan, cancel_at_end, period_end_iso, user_id),
        )
        conn.commit()
    log.info(
        "subscription %s → status=%s plan=%s cancel_at_end=%s for user_id=%s",
        d.get("id"), new_status, new_plan, cancel_at_end, user_id,
    )


@stripe_router.post("/webhook")
async def stripe_webhook(request: Request):
    _ensure_stripe()
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(503, "STRIPE_WEBHOOK_SECRET not configured")

    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    try:
        event = _stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except _stripe.error.SignatureVerificationError:
        raise HTTPException(400, "Invalid webhook signature")
    except Exception as exc:
        raise HTTPException(400, f"Webhook error: {exc}")

    event_type: str = event["type"]
    data_obj = _stripe_to_dict(event["data"]["object"])

    if event_type == "checkout.session.completed":
        # Fired as soon as checkout succeeds — update plan immediately
        sub_id = data_obj.get("subscription")
        if sub_id:
            try:
                sub = _stripe_to_dict(_stripe.Subscription.retrieve(sub_id))
                # Inject user_id from session metadata into sub for reliable lookup
                meta_uid = (data_obj.get("metadata") or {}).get("user_id")
                if meta_uid and not (sub.get("metadata") or {}).get("user_id"):
                    raw_status = sub.get("status", "inactive")
                    new_status = _SUB_STATUS_MAP.get(raw_status, "inactive")
                    new_plan = _plan_from_stripe_sub(sub) if new_status == "active" else "free"
                    with _get_db() as conn:
                        conn.execute(
                            "UPDATE users SET subscription_status = ?, plan = ? WHERE id = ?",
                            (new_status, new_plan, int(meta_uid)),
                        )
                        conn.commit()
                    log.info("checkout.session.completed: user_id=%s → status=%s plan=%s", meta_uid, new_status, new_plan)
                else:
                    _update_subscription_from_stripe(sub)
            except Exception as exc:
                log.warning("checkout.session.completed: error processing sub %s: %s", sub_id, exc, exc_info=True)
        else:
            # No subscription field — may be one-time payment; try customer lookup
            customer_id = data_obj.get("customer")
            meta_uid = (data_obj.get("metadata") or {}).get("user_id")
            if meta_uid:
                # One-time checkout with user_id in metadata — grant starter access
                with _get_db() as conn:
                    conn.execute(
                        "UPDATE users SET subscription_status = 'active', plan = 'starter' WHERE id = ?",
                        (int(meta_uid),),
                    )
                    conn.commit()
                log.info("checkout.session.completed (one-time): user_id=%s → starter", meta_uid)
            elif customer_id:
                with _get_db() as conn:
                    conn.execute(
                        "UPDATE users SET subscription_status = 'active', plan = 'starter' WHERE stripe_customer_id = ?",
                        (customer_id,),
                    )
                    conn.commit()
                log.info("checkout.session.completed (one-time): customer=%s → starter", customer_id)

    elif event_type in (
        "customer.subscription.created",
        "customer.subscription.updated",
        "customer.subscription.deleted",
    ):
        _update_subscription_from_stripe(data_obj)

    elif event_type == "invoice.payment_failed":
        sub_id = data_obj.get("subscription")
        if sub_id:
            try:
                sub = _stripe_to_dict(_stripe.Subscription.retrieve(sub_id))
                _update_subscription_from_stripe(sub)
            except Exception as exc:
                log.warning("Failed to retrieve subscription %s: %s", sub_id, exc)

    elif event_type == "invoice.payment_succeeded":
        sub_id = data_obj.get("subscription")
        if sub_id:
            try:
                sub = _stripe_to_dict(_stripe.Subscription.retrieve(sub_id))
                _update_subscription_from_stripe(sub)
            except Exception as exc:
                log.warning("Failed to retrieve subscription %s: %s", sub_id, exc)

    else:
        # Catch-all: an event type we don't act on was delivered. Log it so a
        # misconfigured "events to send" list in the Stripe Dashboard (or a
        # new event type we should be handling) shows up instead of silently
        # returning 200 with nothing done — this is exactly what masked the
        # 2026-07-15 plan-not-upgraded incident.
        log.warning("stripe webhook: unhandled event type=%s id=%s — no action taken", event_type, event.get("id"))

    return {"received": True}


# ---------------------------------------------------------------------------
# Optional JWT middleware (path-prefix based)
# ---------------------------------------------------------------------------

class JWTMiddleware(BaseHTTPMiddleware):
    """
    Enforces JWT auth on URL prefixes listed in JWT_REQUIRED_PREFIXES env var.
    Add this middleware to the FastAPI app AFTER registering auth_router so that
    /auth/* paths are already known to be exempt.

    Inactive (no-op) when JWT_REQUIRED_PREFIXES is empty.
    """

    async def dispatch(self, request: Request, call_next):
        if not _JWT_PREFIXES:
            return await call_next(request)

        path = request.url.path
        if path in _JWT_EXEMPT or not any(path.startswith(p) for p in _JWT_PREFIXES):
            return await call_next(request)

        # Extract token
        auth_header = request.headers.get("Authorization", "")
        token = ""
        if auth_header.lower().startswith("bearer "):
            token = auth_header[7:].strip()
        if not token:
            return Response(
                content='{"detail":"Missing authentication token"}',
                status_code=401,
                media_type="application/json",
                headers={"WWW-Authenticate": "Bearer"},
            )

        try:
            payload = decode_token(token)
            user_id = int(payload["sub"])
        except (JWTError, KeyError, ValueError):
            return Response(
                content='{"detail":"Invalid or expired token"}',
                status_code=401,
                media_type="application/json",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Check subscription
        with _get_db() as conn:
            user = conn.execute(
                "SELECT subscription_status FROM users WHERE id = ?", (user_id,)
            ).fetchone()

        if user is None or user["subscription_status"] != "active":
            return Response(
                content='{"detail":"Active subscription required"}',
                status_code=403,
                media_type="application/json",
            )

        return await call_next(request)


# ---------------------------------------------------------------------------
# Google + Apple OAuth
# ---------------------------------------------------------------------------

import hashlib
import hmac
import secrets
import urllib.parse

from fastapi.responses import RedirectResponse

GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
APPLE_CLIENT_ID      = os.getenv("APPLE_CLIENT_ID", "")   # Service ID, e.g. com.aurexis.web
APPLE_TEAM_ID        = os.getenv("APPLE_TEAM_ID", "")
APPLE_KEY_ID         = os.getenv("APPLE_KEY_ID", "")
APPLE_PRIVATE_KEY    = os.getenv("APPLE_PRIVATE_KEY", "").replace("\\n", "\n")
APP_BASE_URL         = os.getenv("APP_BASE_URL", "https://aurexis-backend-production.up.railway.app").rstrip("/")
FRONTEND_ORIGIN      = os.getenv("FRONTEND_ORIGIN", "https://useaurexis.com").rstrip("/")

_GOOGLE_AUTH_URL     = "https://accounts.google.com/o/oauth2/v2/auth"
_GOOGLE_TOKEN_URL    = "https://oauth2.googleapis.com/token"
_GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"
_APPLE_AUTH_URL      = "https://appleid.apple.com/auth/authorize"
_APPLE_TOKEN_URL     = "https://appleid.apple.com/auth/token"

oauth_router = APIRouter(prefix="/auth", tags=["oauth"])


# ── CSRF state helpers ────────────────────────────────────────────────────

def _state_make(provider: str, extras: dict | None = None) -> str:
    """Signed state token.
    Old format (3-part): {provider}:{ts}:{hmac16}
    New format (4-part): {provider}:{ts}:{extras_b64}:{hmac16}
    """
    import base64 as _b64, json as _json
    ts = str(int(datetime.now(timezone.utc).timestamp()))
    if extras:
        extras_b64 = _b64.urlsafe_b64encode(_json.dumps(extras).encode()).decode().rstrip("=")
        msg = f"{provider}:{ts}:{extras_b64}".encode()
        sig = hmac.new(JWT_SECRET.encode(), msg, hashlib.sha256).hexdigest()[:16]
        return f"{provider}:{ts}:{extras_b64}:{sig}"
    msg = f"{provider}:{ts}".encode()
    sig = hmac.new(JWT_SECRET.encode(), msg, hashlib.sha256).hexdigest()[:16]
    return f"{provider}:{ts}:{sig}"


def _state_ok(state: str, provider: str, max_age: int = 600) -> bool:
    """Return True if the signed state is valid and not expired.
    Handles both 3-part (legacy) and 4-part (with extras) formats.
    """
    try:
        parts = state.split(":", 3)
        if len(parts) == 3:
            p, ts, sig = parts
            msg = f"{p}:{ts}".encode()
        elif len(parts) == 4:
            p, ts, extras_b64, sig = parts
            msg = f"{p}:{ts}:{extras_b64}".encode()
        else:
            return False
        if p != provider:
            return False
        age = abs(datetime.now(timezone.utc).timestamp() - int(ts))
        if age > max_age:
            return False
        expected = hmac.new(JWT_SECRET.encode(), msg, hashlib.sha256).hexdigest()[:16]
        return hmac.compare_digest(sig, expected)
    except Exception:
        return False


def _state_extras(state: str) -> dict:
    """Extract the extras dict from a 4-part state token. Returns {} otherwise."""
    import base64 as _b64, json as _json
    try:
        parts = state.split(":", 3)
        if len(parts) == 4:
            extras_b64 = parts[2]
            padded = extras_b64 + "=" * (-len(extras_b64) % 4)
            return _json.loads(_b64.urlsafe_b64decode(padded))
    except Exception:
        pass
    return {}


# ── DB helper ────────────────────────────────────────────────────────────

def _upsert_oauth_user(email: str, first_name: str = "", last_name: str = "") -> tuple[sqlite3.Row, bool]:
    """Find or create a user by email (OAuth). Returns (user, is_new)."""
    with _get_db() as conn:
        user = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if user:
            # Backfill name if we now have it but didn't before
            try:
                has_name = bool(user["first_name"])
            except (IndexError, KeyError):
                has_name = False
            if not has_name and first_name:
                conn.execute(
                    "UPDATE users SET first_name = ?, last_name = ? WHERE id = ?",
                    (first_name, last_name or None, user["id"]),
                )
                conn.commit()
                user = conn.execute("SELECT * FROM users WHERE id = ?", (user["id"],)).fetchone()
            return user, False
        now = datetime.now(timezone.utc).isoformat()
        fake_pw = hash_password(secrets.token_urlsafe(32))
        conn.execute(
            "INSERT INTO users (email, password_hash, plan, first_name, last_name, created_at) VALUES (?, ?, 'free', ?, ?, ?)",
            (email, fake_pw, first_name or None, last_name or None, now),
        )
        conn.commit()
        return conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone(), True


def _redirect_to_app(user: sqlite3.Row, is_new: bool = False, origin: str = "") -> RedirectResponse:
    """Issue JWT and redirect to {origin}/app?token=..."""
    plan = _user_plan(user)
    sid = _new_session(user["id"])
    token = create_access_token(user["id"], user["email"], plan=plan, session_id=sid)
    base = (origin or FRONTEND_ORIGIN).rstrip("/")
    url = f"{base}/app?token={urllib.parse.quote(token)}"
    if is_new:
        url += "&new_user=1"
    return RedirectResponse(url=url, status_code=302)


# ── Google ───────────────────────────────────────────────────────────────

_ALLOWED_PLANS = {"free", "starter", "pro", "elite"}


@oauth_router.get("/google/redirect")
def google_redirect(plan: str = "free", origin: str = ""):
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(503, "GOOGLE_CLIENT_ID not configured")
    clean_plan = plan.lower() if plan.lower() in _ALLOWED_PLANS else "free"
    _allowed_origins = {
        FRONTEND_ORIGIN,
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
    }
    clean_origin = origin if origin in _allowed_origins else FRONTEND_ORIGIN
    state = _state_make("google", {"plan": clean_plan, "origin": clean_origin})
    params = urllib.parse.urlencode({
        "client_id":     GOOGLE_CLIENT_ID,
        "redirect_uri":  f"{APP_BASE_URL}/auth/google/callback",
        "response_type": "code",
        "scope":         "openid email profile",
        "state":         state,
        "access_type":   "online",
        "prompt":        "select_account",
    })
    return RedirectResponse(url=f"{_GOOGLE_AUTH_URL}?{params}", status_code=302)


@oauth_router.get("/google/callback")
def google_callback(code: str = "", state: str = "", error: str = ""):
    if error:
        return RedirectResponse(url=f"{FRONTEND_ORIGIN}/auth?error=google_denied", status_code=302)
    if not _state_ok(state, "google"):
        raise HTTPException(400, "INVALID_OAUTH_STATE")
    try:
        import requests as _req
        tok = _req.post(_GOOGLE_TOKEN_URL, data={
            "code":          code,
            "client_id":     GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri":  f"{APP_BASE_URL}/auth/google/callback",
            "grant_type":    "authorization_code",
        }, timeout=10).json()
        access_token = tok.get("access_token", "")
        if not access_token:
            raise ValueError(f"no access_token: {tok}")
        info = _req.get(
            _GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        ).json()
        email = str(info.get("email") or "").strip().lower()
        if not email:
            raise ValueError("no email in Google userinfo")
        first_name = str(info.get("given_name") or "").strip()
        last_name = str(info.get("family_name") or "").strip()
    except Exception as exc:
        log.warning("google_callback error: %s", exc)
        return RedirectResponse(url=f"{FRONTEND_ORIGIN}/auth?error=google_failed", status_code=302)

    user, is_new = _upsert_oauth_user(email, first_name=first_name, last_name=last_name)
    if is_new:
        send_welcome_email_bg(email, first_name)

    extras = _state_extras(state)
    selected_plan = extras.get("plan", "free")
    callback_origin = extras.get("origin", FRONTEND_ORIGIN)

    log.info("google_callback: email=%s is_new=%s selected_plan=%s origin=%s", user["email"], is_new, selected_plan, callback_origin)

    if selected_plan in ("starter", "pro", "elite") and _stripe and STRIPE_SECRET_KEY:
        price_id = PLAN_PRICES.get(selected_plan, "")
        log.info("google_callback: routing to Stripe plan=%s price_id=%s", selected_plan, price_id)
        if price_id:
            try:
                customer_id = user["stripe_customer_id"]
                if not customer_id:
                    customer = _stripe.Customer.create(email=user["email"])
                    customer_id = customer["id"]
                    with _get_db() as conn:
                        conn.execute(
                            "UPDATE users SET stripe_customer_id = ? WHERE id = ?",
                            (customer_id, user["id"]),
                        )
                        conn.commit()
                plan = _user_plan(user)
                sid = _new_session(user["id"])
                token = create_access_token(user["id"], user["email"], plan=plan, session_id=sid)
                success_url = f"{callback_origin}/app?token={urllib.parse.quote(token)}"
                if is_new:
                    success_url += "&new_user=1"
                cancel_url = f"{callback_origin}/signup?plan={selected_plan}"
                session = _stripe.checkout.Session.create(
                    customer=customer_id,
                    payment_method_types=["card"],
                    mode="subscription",
                    line_items=[{"price": price_id, "quantity": 1}],
                    success_url=success_url,
                    cancel_url=cancel_url,
                    metadata={"user_id": str(user["id"])},
                    subscription_data={"metadata": {"user_id": str(user["id"])}},
                )
                checkout_url = session.get("url") or session.get("checkout_url", "")
                log.info("google_callback: redirecting to Stripe checkout url=%s", checkout_url[:60] if checkout_url else "NONE")
                if checkout_url:
                    return RedirectResponse(url=checkout_url, status_code=302)
                log.warning("google_callback: Stripe session had no url, falling through")
            except Exception as exc:
                log.warning("google_callback stripe checkout error: %s", exc, exc_info=True)
        else:
            log.warning("google_callback: no price_id for plan=%s PLAN_PRICES=%s", selected_plan, PLAN_PRICES)
    else:
        log.info("google_callback: skipping Stripe — plan=%s stripe=%s key=%s", selected_plan, bool(_stripe), bool(STRIPE_SECRET_KEY))

    return _redirect_to_app(user, is_new=is_new, origin=callback_origin)


# ── Apple ────────────────────────────────────────────────────────────────

def _apple_client_secret() -> str:
    """
    Apple requires a JWT signed with your ES256 private key as the client_secret.
    Valid up to 6 months. Requires APPLE_TEAM_ID, APPLE_KEY_ID, APPLE_PRIVATE_KEY.
    """
    now = datetime.now(timezone.utc)
    payload = {
        "iss": APPLE_TEAM_ID,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(days=180)).timestamp()),
        "aud": "https://appleid.apple.com",
        "sub": APPLE_CLIENT_ID,
    }
    return jwt.encode(payload, APPLE_PRIVATE_KEY, algorithm="ES256", headers={"kid": APPLE_KEY_ID})


@oauth_router.get("/apple/redirect")
def apple_redirect():
    if not APPLE_CLIENT_ID:
        raise HTTPException(503, "APPLE_CLIENT_ID not configured")
    state = _state_make("apple")
    params = urllib.parse.urlencode({
        "client_id":     APPLE_CLIENT_ID,
        "redirect_uri":  f"{APP_BASE_URL}/auth/apple/callback",
        "response_type": "code",
        "scope":         "name email",
        "state":         state,
        "response_mode": "form_post",
    })
    return RedirectResponse(url=f"{_APPLE_AUTH_URL}?{params}", status_code=302)


@oauth_router.post("/apple/callback")
async def apple_callback(request: Request):
    """Apple sends the callback as an HTTP form POST (not GET)."""
    form = await request.form()
    code  = str(form.get("code", ""))
    state = str(form.get("state", ""))
    error = str(form.get("error", ""))

    if error:
        return RedirectResponse(url=f"{FRONTEND_ORIGIN}/auth?error=apple_denied", status_code=302)
    if not _state_ok(state, "apple"):
        raise HTTPException(400, "INVALID_OAUTH_STATE")

    try:
        import requests as _req
        tok = _req.post(_APPLE_TOKEN_URL, data={
            "client_id":     APPLE_CLIENT_ID,
            "client_secret": _apple_client_secret(),
            "code":          code,
            "grant_type":    "authorization_code",
            "redirect_uri":  f"{APP_BASE_URL}/auth/apple/callback",
        }, timeout=10).json()

        id_token = tok.get("id_token", "")
        if not id_token:
            raise ValueError(f"no id_token: {tok}")

        # Decode claims without verifying signature —
        # the token exchange itself (authenticated with our client_secret) proves authenticity
        claims = jwt.get_unverified_claims(id_token)
        email = str(claims.get("email") or "").strip().lower()
        if not email:
            # Apple private relay — build a stable synthetic email from the user's sub
            sub = str(claims.get("sub") or secrets.token_hex(8))
            email = f"{sub}@privaterelay.appleid.com"

    except Exception as exc:
        log.warning("apple_callback error: %s", exc)
        return RedirectResponse(url=f"{FRONTEND_ORIGIN}/auth?error=apple_failed", status_code=302)

    user, is_new = _upsert_oauth_user(email)
    return _redirect_to_app(user, is_new=is_new)
