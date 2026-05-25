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

JWT_SECRET = os.getenv("JWT_SECRET_KEY", "CHANGE_ME_insecure_default_key_32chars!")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_DAYS = 30

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")

PLAN_PRICES = {
    "starter": os.getenv("STRIPE_PRICE_STARTER", ""),
    "pro": os.getenv("STRIPE_PRICE_PRO", ""),
    "elite": os.getenv("STRIPE_PRICE_ELITE", ""),
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
                created_at          TEXT    NOT NULL
            )
        """)
        conn.commit()
    log.info("auth.db initialised")


# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(plain: str) -> str:
    return _pwd_ctx.hash(plain[:72])


def verify_password(plain: str, hashed: str) -> bool:
    return _pwd_ctx.verify(plain, hashed)


# ---------------------------------------------------------------------------
# JWT helpers
# ---------------------------------------------------------------------------

def create_access_token(user_id: int, email: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS)
    payload = {"sub": str(user_id), "email": email, "exp": expire}
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    """Decode and validate a JWT. Raises JWTError on failure."""
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])


# ---------------------------------------------------------------------------
# FastAPI security dependency
# ---------------------------------------------------------------------------

_bearer = HTTPBearer(auto_error=False)


def _extract_token(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
    authorization: Optional[str] = Header(default=None),
) -> str:
    """Pull token from Bearer header. Returns raw token string or raises 401."""
    token: Optional[str] = None
    if creds:
        token = creds.credentials
    elif authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
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


# ---------------------------------------------------------------------------
# Auth router
# ---------------------------------------------------------------------------

auth_router = APIRouter(prefix="/auth", tags=["auth"])


@auth_router.post("/signup", status_code=201)
def signup(body: SignupRequest):
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

    token = create_access_token(user_id, body.email)
    return {"access_token": token, "token_type": "bearer", "user_id": user_id, "email": body.email}


@auth_router.post("/login")
def login(body: LoginRequest):
    with _get_db() as conn:
        user = conn.execute(
            "SELECT * FROM users WHERE email = ?", (body.email,)
        ).fetchone()

    if user is None or not verify_password(body.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )

    token = create_access_token(user["id"], user["email"])
    return {
        "access_token": token,
        "token_type": "bearer",
        "user_id": user["id"],
        "email": user["email"],
        "subscription_status": user["subscription_status"],
    }


@auth_router.get("/me")
def me(user: sqlite3.Row = Depends(get_current_user)):
    return {
        "id": user["id"],
        "email": user["email"],
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

    session = _stripe.checkout.Session.create(
        customer=customer_id,
        payment_method_types=["card"],
        mode="subscription",
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=body.success_url,
        cancel_url=body.cancel_url,
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


def _update_subscription_from_stripe(stripe_sub) -> None:
    """Persist subscription status change to the users table."""
    raw_status = stripe_sub.get("status", "inactive")
    new_status = _SUB_STATUS_MAP.get(raw_status, "inactive")

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
            "UPDATE users SET subscription_status = ? WHERE id = ?",
            (new_status, user_id),
        )
        conn.commit()
    log.info("subscription %s → status=%s for user_id=%s", stripe_sub.get("id"), new_status, user_id)


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
