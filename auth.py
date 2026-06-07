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
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, EmailStr
from starlette.middleware.base import BaseHTTPMiddleware

from jose import JWTError, jwt
from passlib.context import CryptContext

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

_AUTH_DB_PATH = os.path.join(os.path.dirname(__file__), "auth.db")


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
            ("plan",       "TEXT NOT NULL DEFAULT 'free'"),
            ("first_name", "TEXT"),
            ("last_name",  "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE users ADD COLUMN {col} {defn}")
            except Exception:
                pass  # column already exists
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
_SENDGRID_KEY  = os.getenv("SENDGRID_API_KEY", "")
_FRONTEND_URL  = os.getenv("FRONTEND_ORIGIN", "https://useaurexis.com")


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


def _send_welcome_email(to_email: str, first_name: str = "") -> None:
    """Fire-and-forget welcome email. Called in a background thread."""
    subject = "Welcome to Aurexis — your edge starts now"
    html = _welcome_html(first_name)
    plain = f"Hey {first_name or 'there'},\n\nWelcome to Aurexis. Every morning we scan 1,200+ stocks and surface one high-conviction trade setup with entry, stop, and targets.\n\nOpen the app: {_FRONTEND_URL}/app\n\nAurexis"

    # Try SendGrid first
    if _SENDGRID_KEY:
        try:
            import urllib.request as _ur, json as _json
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
                headers={"Authorization": f"Bearer {_SENDGRID_KEY}", "Content-Type": "application/json"},
                method="POST",
            )
            _ur.urlopen(req, timeout=10)
            log.info("Welcome email sent via SendGrid to %s", to_email)
            return
        except Exception as exc:
            log.warning("SendGrid welcome email failed: %s", exc)

    # Fall back to SMTP
    if not _SMTP_HOST or not _SMTP_USER or not _SMTP_PASS:
        log.info("No email credentials configured — skipping welcome email for %s", to_email)
        return
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
        log.info("Welcome email sent via SMTP to %s", to_email)
    except Exception as exc:
        log.warning("SMTP welcome email failed: %s", exc)


def send_welcome_email_bg(to_email: str, first_name: str = "") -> None:
    """Send welcome email in a background thread — never blocks the request."""
    threading.Thread(target=_send_welcome_email, args=(to_email, first_name), daemon=True).start()


# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    return _pwd_ctx.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    return _pwd_ctx.verify(plain, hashed)


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------

PLAN_RANK = {"free": 0, "starter": 1, "pro": 2, "elite": 3}


def create_access_token(user_id: int, email: str, plan: str = "free") -> str:
    expire = datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS)
    payload = {"sub": str(user_id), "email": email, "plan": plan, "exp": expire}
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

    token = create_access_token(user_id, body.email, plan="free")
    _set_auth_cookie(response, token)
    send_welcome_email_bg(body.email, first)
    return {"access_token": token, "token_type": "bearer", "user_id": user_id, "email": body.email, "plan": "free", "first_name": first}


@auth_router.post("/login")
def login(body: LoginRequest, response: Response):
    with _get_db() as conn:
        user = conn.execute(
            "SELECT * FROM users WHERE email = ?", (body.email,)
        ).fetchone()

    if user is None or not verify_password(body.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    plan = _user_plan(user)
    token = create_access_token(user["id"], user["email"], plan=plan)
    _set_auth_cookie(response, token)
    return {
        "access_token": token,
        "token_type": "bearer",
        "user_id": user["id"],
        "email": user["email"],
        "plan": plan,
        "subscription_status": user["subscription_status"],
    }


@auth_router.post("/logout")
def logout(response: Response):
    """Clear the httpOnly session cookie."""
    response.delete_cookie(key="sq_token", path="/", httponly=True, secure=_IS_PROD, samesite="lax")
    return {"ok": True}


@auth_router.get("/me")
def me(user: sqlite3.Row = Depends(get_current_user)):
    def _safe(col):
        try: return user[col]
        except (IndexError, KeyError): return None
    return {
        "id": user["id"],
        "email": user["email"],
        "plan": _user_plan(user),
        "subscription_status": user["subscription_status"],
        "stripe_customer_id": user["stripe_customer_id"],
        "created_at": user["created_at"],
        "first_name": _safe("first_name"),
        "last_name": _safe("last_name"),
    }


@auth_router.delete("/delete-account")
def delete_account(response: Response, user: sqlite3.Row = Depends(get_current_user)):
    with _get_db() as conn:
        conn.execute("DELETE FROM users WHERE id = ?", (user["id"],))
        conn.commit()
    response.delete_cookie(key="sq_token", path="/", httponly=True, secure=_IS_PROD, samesite="lax")
    return {"ok": True}


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
    )
    return {"checkout_url": session["url"], "session_id": session["id"]}


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


def _plan_from_stripe_sub(stripe_sub) -> str:
    """Extract our plan string from a Stripe subscription object."""
    try:
        items = stripe_sub.get("items", {}).get("data", [])
        for item in items:
            price_id = item.get("price", {}).get("id", "")
            if price_id in _PRICE_TO_PLAN:
                return _PRICE_TO_PLAN[price_id]
    except Exception:
        pass
    return "free"


def _update_subscription_from_stripe(stripe_sub) -> None:
    """Persist subscription status and plan change to the users table."""
    raw_status = stripe_sub.get("status", "inactive")
    new_status = _SUB_STATUS_MAP.get(raw_status, "inactive")
    new_plan = _plan_from_stripe_sub(stripe_sub) if new_status == "active" else "free"

    # Prefer metadata user_id, fall back to customer lookup
    user_id: Optional[int] = None
    meta_uid = (stripe_sub.get("metadata") or {}).get("user_id")
    if meta_uid:
        try:
            user_id = int(meta_uid)
        except ValueError:
            pass

    if user_id is None:
        customer_id = stripe_sub.get("customer")
        if customer_id:
            with _get_db() as conn:
                row = conn.execute(
                    "SELECT id FROM users WHERE stripe_customer_id = ?", (customer_id,)
                ).fetchone()
                if row:
                    user_id = row["id"]

    if user_id is None:
        log.warning("stripe webhook: could not resolve user for subscription %s", stripe_sub.get("id"))
        return

    with _get_db() as conn:
        conn.execute(
            "UPDATE users SET subscription_status = ?, plan = ? WHERE id = ?",
            (new_status, new_plan, user_id),
        )
        conn.commit()
    log.info(
        "subscription %s → status=%s plan=%s for user_id=%s",
        stripe_sub.get("id"), new_status, new_plan, user_id,
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
    data_obj = event["data"]["object"]

    if event_type in (
        "customer.subscription.created",
        "customer.subscription.updated",
        "customer.subscription.deleted",
    ):
        _update_subscription_from_stripe(data_obj)

    elif event_type == "invoice.payment_failed":
        sub_id = data_obj.get("subscription")
        if sub_id:
            try:
                sub = _stripe.Subscription.retrieve(sub_id)
                _update_subscription_from_stripe(sub)
            except Exception as exc:
                log.warning("Failed to retrieve subscription %s: %s", sub_id, exc)

    elif event_type == "invoice.payment_succeeded":
        sub_id = data_obj.get("subscription")
        if sub_id:
            try:
                sub = _stripe.Subscription.retrieve(sub_id)
                _update_subscription_from_stripe(sub)
            except Exception as exc:
                log.warning("Failed to retrieve subscription %s: %s", sub_id, exc)

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

def _state_make(provider: str) -> str:
    """Signed state token: {provider}:{ts}:{hmac16}"""
    ts = str(int(datetime.now(timezone.utc).timestamp()))
    msg = f"{provider}:{ts}".encode()
    sig = hmac.new(JWT_SECRET.encode(), msg, hashlib.sha256).hexdigest()[:16]
    return f"{provider}:{ts}:{sig}"


def _state_ok(state: str, provider: str, max_age: int = 600) -> bool:
    """Return True if the signed state is valid and not expired."""
    try:
        parts = state.rsplit(":", 2)
        if len(parts) != 3:
            return False
        p, ts, sig = parts
        if p != provider:
            return False
        age = abs(datetime.now(timezone.utc).timestamp() - int(ts))
        if age > max_age:
            return False
        msg = f"{provider}:{ts}".encode()
        expected = hmac.new(JWT_SECRET.encode(), msg, hashlib.sha256).hexdigest()[:16]
        return hmac.compare_digest(sig, expected)
    except Exception:
        return False


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


def _redirect_to_app(user: sqlite3.Row, is_new: bool = False) -> RedirectResponse:
    """Issue JWT and redirect to {FRONTEND_ORIGIN}/app?token=..."""
    plan = _user_plan(user)
    token = create_access_token(user["id"], user["email"], plan=plan)
    url = f"{FRONTEND_ORIGIN}/app?token={urllib.parse.quote(token)}"
    if is_new:
        url += "&new_user=1"
    return RedirectResponse(url=url, status_code=302)


# ── Google ───────────────────────────────────────────────────────────────

@oauth_router.get("/google/redirect")
def google_redirect():
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(503, "GOOGLE_CLIENT_ID not configured")
    state = _state_make("google")
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
    return _redirect_to_app(user, is_new=is_new)


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
