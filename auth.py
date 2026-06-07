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
from datetime import datetime, timedelta, timezone
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
                created_at          TEXT    NOT NULL
            )
        """)
        # Non-destructive migration: add plan column to existing databases
        try:
            conn.execute("ALTER TABLE users ADD COLUMN plan TEXT NOT NULL DEFAULT 'free'")
        except Exception:
            pass  # column already exists
        conn.commit()
    log.info("auth.db initialised")


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
        user_plan = str(user["plan"] or "free").lower()
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
    try:
        with _get_db() as conn:
            cur = conn.execute(
                "INSERT INTO users (email, password_hash, created_at) VALUES (?, ?, ?)",
                (body.email, pw_hash, now),
            )
            conn.commit()
            user_id = cur.lastrowid
    except sqlite3.IntegrityError:
        raise HTTPException(409, "Email already registered")

    token = create_access_token(user_id, body.email, plan="free")
    _set_auth_cookie(response, token)
    return {"access_token": token, "token_type": "bearer", "user_id": user_id, "email": body.email, "plan": "free"}


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

    plan = str(user["plan"] or "free").lower()
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
    return {
        "id": user["id"],
        "email": user["email"],
        "plan": str(user["plan"] or "free"),
        "subscription_status": user["subscription_status"],
        "stripe_customer_id": user["stripe_customer_id"],
        "created_at": user["created_at"],
    }


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
