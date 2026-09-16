"""
Growth Agent — the "AI ad agency" module.

One brand brain, multiple content generators (SEO, social, email, ad copy),
all writing into a single approval queue instead of five disconnected tools.
Nothing here auto-publishes or spends money — every item lands as "pending"
and a human (admin secret) approves it, and *separately* clicks publish,
before anything goes out. Real, working publish paths: Reddit (a "script"
app's credentials, no platform review needed) and your own blog (self-
published, no external platform at all). TikTok/Instagram/ad-platform
posting is intentionally NOT wired to actually post yet -- those platforms
require you to register a developer app and clear their own review first;
see PLATFORM_SETUP_NOTES for what each one needs. Never spends money --
nothing here touches an ads-buying endpoint.

Grounding: every generator gets the same BRAND_FACTS block and the same
compliance rules AURO (_CHAT_SYSTEM in app.py) already runs on. Aurexis is a
trading app -- an LLM writing ad copy with no constraints WILL invent a stat
or promise a return, exactly like it invented an "Elite" tier for AURO
before that got fixed tonight (see git log). The fix there was giving the
model real facts and telling it not to invent more; same fix, same file
family, same reason.
"""

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from llm_client import call_llm_text, llm_available, LLMDisabledError, LLMCircuitOpenError, LLMDailyCapExceededError, LLMCallError


class PublishError(Exception):
    """Raised when an actual publish (Reddit post, etc.) fails or isn't wired up yet."""
    pass


# ---------------------------------------------------------------------------
# Brand brain — single source of truth every generator is grounded in.
# Keep this in sync with src/lib/pricing.js / Landing.jsx (frontend) and
# PLAN_DISPLAY (auth.py) -- same discipline as the AURO plan-list fix.
# ---------------------------------------------------------------------------

BRAND_FACTS = """
PRODUCT: Aurexis (useaurexis.com) — an AI-powered stock pick app. iOS app + web.

PLANS (the complete list — there is no Elite/Premium/Plus/Enterprise tier):
- Free — $0: 1 AI pick/month, 3 stock analyses/day, market regime indicator, Top Movers (5 tickers).
- Starter — $9/month: 3 picks/week, edge signals, "Why This Trade" reasoning, AI news & sentiment
  summary, full Top Movers, watchlist + trade journal + performance tracking, email alerts.
- Pro — $29/month: everything in Starter, plus full trade plan (entry/stop/Fibonacci targets),
  unlimited daily AI picks, position sizing recommendations, multi-ticker screener, portfolio tracking.

WHAT THE PRODUCT ACTUALLY DOES: scans the market with a neural-network scorer (0-10 scale, edge
signals, regime-aware), surfaces trade ideas with an AI-generated score and reasoning. AURO is the
in-app AI chat assistant for questions about picks/plans/market concepts.

COMPLIANCE — NON-NEGOTIABLE, applies to every piece of copy this agent writes:
- Aurexis is NOT investment advice. Not a registered investment advisor. Frame everything as
  analysis/education, never as a promise or recommendation to buy/sell.
- NEVER state or imply a guaranteed return, win rate, or performance number unless it was
  explicitly given to you in this prompt's TOPIC/BRIEF section for this specific piece — do not
  invent a percentage, a dollar figure, or a testimonial. "Past performance is not indicative of
  future results" is the house rule, not a suggestion.
- NEVER invent a feature, plan, or price not listed above. If unsure whether something is real,
  don't claim it.
- No countdown-timer/fake-scarcity tactics ("only 3 spots left") — there's no factual basis for them.
- THERE IS NO FREE TRIAL. Do not write "free trial," "try free for X days," or similar — the Free
  plan (permanent, $0/month, listed above) is the free option. Do not blur the two.
- Do not claim a specific win rate, return %, or "accuracy" figure anywhere in ad/marketing copy —
  even a plausible-sounding one — unless it was handed to you verbatim in this prompt's BRIEF.
"""

_VALID_TYPES = {"seo_post", "social_caption", "email_sequence", "ad_copy"}
_VALID_PLATFORMS = {"tiktok", "instagram", "x", "linkedin", "email", "google_search", "blog", "reddit", "none"}

# Defense-in-depth: the system prompt telling the model not to hallucinate
# claims is not reliable enough on its own -- verified live, the very first
# ad-copy test still wrote "Start Your Free Trial Today!" despite the brief
# already banning it. This regex pass runs on every generated item and
# stores what it caught as `flags` on the queue row, so a reviewer sees
# "⚠ contains a banned phrase" instead of having to independently notice a
# wall of text got something wrong. It does not block saving the item --
# everything still lands as 'pending' for a human to actually decide -- it
# just makes the review meaningfully faster and harder to rubber-stamp.
_LINT_PATTERNS = [
    (r"free trial|try (it )?free for \d+", "claims a free trial (doesn't exist -- there's a permanent Free plan, not a trial)"),
    # "Plus" deliberately excluded -- too common a word ("Plus, you'll get...") to use as a
    # lint signal; caught a real false positive on it during testing. Elite/Premium/Enterprise
    # are rare enough in ordinary marketing prose that a match is a real signal.
    (r"\bElite\b|\bPremium\b|\bEnterprise\b", "mentions a plan tier that doesn't exist"),
    (r"\d{1,3}(\.\d+)?\s*%\s*(win rate|accuracy|return|success)", "states a specific win-rate/return/accuracy percentage"),
    (r"guarantee[ds]?\b", "uses \"guarantee(d)\" language about outcomes"),
    (r"only \d+ (spots?|seats?|left)", "fake-scarcity claim"),
]


def _lint_content(body: str) -> List[str]:
    import re
    warnings = []
    for pattern, msg in _LINT_PATTERNS:
        if re.search(pattern, body, re.IGNORECASE):
            warnings.append(msg)
    return warnings


def _db_path() -> str:
    return os.getenv("STACKIQ_DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "stackiq.db"))


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn


def ensure_growth_schema() -> None:
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS growth_content (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                content_type  TEXT    NOT NULL,   -- seo_post | social_caption | email_sequence | ad_copy
                platform      TEXT    NOT NULL DEFAULT 'none',
                topic         TEXT    NOT NULL DEFAULT '',
                brief         TEXT,               -- optional extra instructions/facts for this item
                body           TEXT    NOT NULL DEFAULT '',
                flags         TEXT,               -- JSON array of lint warnings, see _lint_content
                status        TEXT    NOT NULL DEFAULT 'pending',  -- pending | approved | rejected | published
                created_at    TEXT    NOT NULL,
                reviewed_at   TEXT,
                review_notes  TEXT
            )
        """)
        for col, defn in [
            ("flags", "TEXT"),
            # Set only once an approved item is actually published somewhere real
            # (see publish_item) -- distinct from 'approved', which only means a
            # human signed off on the text, not that it went anywhere.
            ("published_at", "TEXT"),
            ("published_platform", "TEXT"),
            ("published_url", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE growth_content ADD COLUMN {col} {defn}")
            except Exception:
                pass  # column already exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS platform_connections (
                platform      TEXT PRIMARY KEY,   -- 'reddit' | 'tiktok' | 'instagram' | ...
                credentials   TEXT NOT NULL,       -- JSON blob, shape is platform-specific
                connected_at  TEXT NOT NULL
            )
        """)
        conn.commit()


_SYSTEM_BY_TYPE = {
    "seo_post": (
        "You write SEO blog content for Aurexis. Output: an SEO-friendly title, a meta description "
        "(<=155 chars), and a 500-800 word post in Markdown with H2 subheadings. Direct, concrete, "
        "no hype/filler. Educational tone about trading/investing concepts or how Aurexis's scoring "
        "works -- never a hard sell."
    ),
    "social_caption": (
        "You write short-form social copy for Aurexis (the platform is given in TOPIC/BRIEF). For "
        "TikTok, write a script outline: hook line, 3-5 beats, on-screen text suggestions, and a caption "
        "with hashtags -- you are NOT producing video, just the script/creative brief a human films from. "
        "For X/LinkedIn/Instagram, write the actual post copy. For Reddit specifically: write like a "
        "person genuinely discussing the topic, not an ad -- no hashtags, no CTA-first framing, no "
        "'check out our app' opener. Reddit's culture actively despises obvious marketing and will "
        "downvote/ban it; a post only works there if it reads as a real contribution to the subreddit's "
        "topic, with the Aurexis mention (if any) secondary and disclosed, not the point of the post. "
        "For every platform: keep it punchy, concrete, no vague hype."
    ),
    "email_sequence": (
        "You write one email in a lifecycle sequence for Aurexis (the moment/segment is given in "
        "TOPIC/BRIEF, e.g. 'day 3 after signup, still on Free' or 'Starter user, used all 3 weekly "
        "picks'). Output: subject line, preview text, and the email body (plain text, conversational, "
        "one clear CTA). Personalize to the segment described, don't invent data about the specific "
        "recipient beyond what's in the brief."
    ),
    "ad_copy": (
        "You write ad copy DRAFTS for Aurexis for the platform given in TOPIC/BRIEF (e.g. TikTok Ads, "
        "Meta Ads, Google Search). Output: 3 headline variants, 2 body-copy variants, and a suggested "
        "primary CTA. This is copy only -- no targeting, no budget, no claims about audience or spend. "
        "Label clearly this is a draft for human review before any campaign is created."
    ),
}


def generate_content(content_type: str, topic: str, platform: str = "none", brief: str = "") -> Dict[str, Any]:
    """
    Generate one piece of content, grounded in BRAND_FACTS, and save it to
    the approval queue as 'pending'. Returns the saved row. Raises ValueError
    on bad input, or an LLM*Error subclass if the model call itself fails
    (caller decides how to surface that -- see app.py endpoint).
    """
    ensure_growth_schema()

    content_type = str(content_type or "").strip().lower()
    platform = str(platform or "none").strip().lower()
    topic = str(topic or "").strip()

    if content_type not in _VALID_TYPES:
        raise ValueError(f"content_type must be one of {sorted(_VALID_TYPES)}")
    if platform not in _VALID_PLATFORMS:
        raise ValueError(f"platform must be one of {sorted(_VALID_PLATFORMS)}")
    if not topic:
        raise ValueError("topic is required")
    if not llm_available():
        raise LLMDisabledError("LLM not configured")

    system = (
        f"{_SYSTEM_BY_TYPE[content_type]}\n\n"
        f"BRAND FACTS (ground truth -- do not contradict or invent beyond this):\n{BRAND_FACTS}"
    )
    user = f"TOPIC/BRIEF: {topic}"
    if platform != "none":
        user += f"\nPLATFORM: {platform}"
    if brief:
        user += f"\nADDITIONAL BRIEF: {brief}"

    body = call_llm_text(
        system=system,
        user=user,
        max_output_tokens=1200,
        timeout_s=45.0,
        label=f"growth_{content_type}",
    )

    warnings = _lint_content(body)
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        cur = conn.execute(
            """INSERT INTO growth_content (content_type, platform, topic, brief, body, flags, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)""",
            (content_type, platform, topic, brief, body, json.dumps(warnings), now),
        )
        conn.commit()
        row_id = cur.lastrowid

    return {
        "id": row_id, "content_type": content_type, "platform": platform,
        "topic": topic, "brief": brief, "body": body, "flags": warnings,
        "status": "pending", "created_at": now,
    }


def list_queue(status: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    ensure_growth_schema()
    q = "SELECT * FROM growth_content"
    params: tuple = ()
    if status:
        q += " WHERE status = ?"
        params = (status,)
    q += " ORDER BY created_at DESC LIMIT ?"
    params = params + (int(limit),)
    with _connect() as conn:
        rows = conn.execute(q, params).fetchall()
    return [_decode_flags(dict(r)) for r in rows]


def _decode_flags(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = row.get("flags")
    try:
        row["flags"] = json.loads(raw) if raw else []
    except Exception:
        row["flags"] = []
    return row


def review_item(item_id: int, action: str, notes: str = "") -> Dict[str, Any]:
    ensure_growth_schema()
    action = str(action or "").strip().lower()
    if action not in ("approve", "reject"):
        raise ValueError("action must be 'approve' or 'reject'")
    status = "approved" if action == "approve" else "rejected"
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE growth_content SET status = ?, reviewed_at = ?, review_notes = ? WHERE id = ?",
            (status, now, notes, int(item_id)),
        )
        conn.commit()
        if cur.rowcount == 0:
            raise ValueError(f"no growth_content row with id={item_id}")
        row = conn.execute("SELECT * FROM growth_content WHERE id = ?", (int(item_id),)).fetchone()
    return _decode_flags(dict(row))


# ---------------------------------------------------------------------------
# Platform connections — where you plug in real credentials for a platform,
# once you have them. Storage only; nothing here creates an app/account on
# any platform for you (can't be done via API -- each one requires you to
# register a developer app on their own site first).
# ---------------------------------------------------------------------------

# What each platform needs, and whether posting is actually wired up yet.
# 'live': publish_item() can really post there today.
# 'needs_review': the platform's own app-review process (not a technical gap
#   here) has to clear before their API will accept a real post -- see
#   PLATFORM_SETUP_NOTES.
PLATFORM_FIELDS = {
    "reddit":     {"fields": ["client_id", "client_secret", "username", "password", "subreddit"], "status": "live"},
    "tiktok":     {"fields": ["access_token", "advertiser_id"], "status": "needs_review"},
    "instagram":  {"fields": ["access_token", "ig_user_id"], "status": "needs_review"},
    "meta_ads":   {"fields": ["access_token", "ad_account_id"], "status": "needs_review"},
    "google_ads": {"fields": ["developer_token", "customer_id", "refresh_token"], "status": "needs_review"},
}

PLATFORM_SETUP_NOTES = {
    "reddit": "Create a 'script' app at reddit.com/prefs/apps (instant, no review). "
              "client_id is under the app name, client_secret is the 'secret' field.",
    "tiktok": "Requires a TikTok for Developers app with Content Posting API access -- "
              "TikTok reviews and approves this per-app before it can post to a real "
              "account (not something an API call can skip).",
    "instagram": "Requires a Meta Developer app + an Instagram Business account linked to "
                 "a Facebook Page, and Meta's app review for the instagram_content_publish "
                 "permission before posting works on a real account.",
    "meta_ads": "Requires a Meta Ads account with billing set up, a Meta Developer app, "
                "and (depending on spend) Meta's business verification.",
    "google_ads": "Requires a Google Ads account, a developer token (Google approves the "
                   "token tier), and OAuth credentials for that account.",
}


def save_connection(platform: str, credentials: Dict[str, Any]) -> Dict[str, Any]:
    ensure_growth_schema()
    platform = str(platform or "").strip().lower()
    if platform not in PLATFORM_FIELDS:
        raise ValueError(f"platform must be one of {sorted(PLATFORM_FIELDS)}")
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            """INSERT INTO platform_connections (platform, credentials, connected_at) VALUES (?, ?, ?)
               ON CONFLICT(platform) DO UPDATE SET credentials = excluded.credentials, connected_at = excluded.connected_at""",
            (platform, json.dumps(credentials), now),
        )
        conn.commit()
    return {"platform": platform, "connected_at": now}


def get_connection(platform: str) -> Optional[Dict[str, Any]]:
    ensure_growth_schema()
    with _connect() as conn:
        row = conn.execute("SELECT * FROM platform_connections WHERE platform = ?", (platform,)).fetchone()
    if not row:
        return None
    try:
        creds = json.loads(row["credentials"])
    except Exception:
        creds = {}
    return {"platform": row["platform"], "connected_at": row["connected_at"], "credentials": creds}


def _redact(creds: Dict[str, Any]) -> Dict[str, str]:
    """Show enough to confirm what's connected without ever returning a secret to the UI."""
    out = {}
    for k, v in (creds or {}).items():
        s = str(v or "")
        if any(tag in k.lower() for tag in ("secret", "password", "token")):
            out[k] = f"••••{s[-4:]}" if len(s) >= 4 else "••••"
        else:
            out[k] = s
    return out


def list_connections() -> List[Dict[str, Any]]:
    ensure_growth_schema()
    with _connect() as conn:
        rows = conn.execute("SELECT * FROM platform_connections").fetchall()
    connected = {r["platform"]: r for r in rows}
    out = []
    for platform, meta in PLATFORM_FIELDS.items():
        row = connected.get(platform)
        creds = {}
        if row:
            try:
                creds = json.loads(row["credentials"])
            except Exception:
                creds = {}
        out.append({
            "platform": platform,
            "status": meta["status"],
            "required_fields": meta["fields"],
            "setup_note": PLATFORM_SETUP_NOTES.get(platform, ""),
            "connected": row is not None,
            "connected_at": row["connected_at"] if row else None,
            "credentials_redacted": _redact(creds) if row else {},
        })
    return out


def delete_connection(platform: str) -> None:
    ensure_growth_schema()
    with _connect() as conn:
        conn.execute("DELETE FROM platform_connections WHERE platform = ?", (platform,))
        conn.commit()


# ---------------------------------------------------------------------------
# Publishing — the actual outbound action. Only ever fired by an explicit
# human click (see /admin/growth/publish), and only on an item already
# 'approved'. Distinct from approve/reject, which never leaves this DB.
# ---------------------------------------------------------------------------

def _publish_to_reddit(item: Dict[str, Any], creds: Dict[str, Any]) -> str:
    client_id = creds.get("client_id", "")
    client_secret = creds.get("client_secret", "")
    username = creds.get("username", "")
    password = creds.get("password", "")
    subreddit = creds.get("subreddit", "")
    if not all([client_id, client_secret, username, password, subreddit]):
        raise PublishError("Reddit connection is missing one of client_id/client_secret/username/password/subreddit.")

    # Script-app OAuth password grant -- the simplest Reddit auth flow, meant
    # exactly for a single account posting as itself (not a multi-user OAuth
    # redirect flow, which would be overkill for "I connect my own account").
    try:
        auth_resp = requests.post(
            "https://www.reddit.com/api/v1/access_token",
            auth=(client_id, client_secret),
            data={"grant_type": "password", "username": username, "password": password},
            headers={"User-Agent": "aurexis-growth-agent/1.0"},
            timeout=15,
        )
    except requests.RequestException as e:
        raise PublishError(f"Reddit auth request failed: {e}")
    if not auth_resp.ok:
        raise PublishError(f"Reddit auth failed ({auth_resp.status_code}): {auth_resp.text[:200]}")
    token = auth_resp.json().get("access_token")
    if not token:
        raise PublishError("Reddit auth succeeded but returned no access_token.")

    # Reddit posts are plain title + text, not our Markdown-with-meta-description
    # SEO format -- use the topic as the title and the full body as self-text.
    try:
        submit_resp = requests.post(
            "https://oauth.reddit.com/api/submit",
            headers={"Authorization": f"bearer {token}", "User-Agent": "aurexis-growth-agent/1.0"},
            data={
                "sr": subreddit, "kind": "self",
                "title": item.get("topic", "")[:300],
                "text": item.get("body", ""),
                "api_type": "json",
            },
            timeout=20,
        )
    except requests.RequestException as e:
        raise PublishError(f"Reddit submit request failed: {e}")
    if not submit_resp.ok:
        raise PublishError(f"Reddit submit failed ({submit_resp.status_code}): {submit_resp.text[:200]}")
    data = submit_resp.json()
    errors = data.get("json", {}).get("errors") or []
    if errors:
        raise PublishError(f"Reddit rejected the post: {errors}")
    url = (data.get("json", {}).get("data", {}) or {}).get("url", "")
    if not url:
        raise PublishError(f"Reddit submit returned no post URL -- raw response: {json.dumps(data)[:300]}")
    return url


def publish_item(item_id: int, platform: str) -> Dict[str, Any]:
    ensure_growth_schema()
    platform = str(platform or "").strip().lower()

    with _connect() as conn:
        row = conn.execute("SELECT * FROM growth_content WHERE id = ?", (int(item_id),)).fetchone()
    if not row:
        raise ValueError(f"no growth_content row with id={item_id}")
    item = _decode_flags(dict(row))
    if item["status"] != "approved":
        raise PublishError(f"item #{item_id} is '{item['status']}', not 'approved' -- approve it first.")

    if platform == "blog":
        # Self-publish: no external platform, just flips visibility on the
        # public /growth/blog listing this same backend serves.
        url = f"/blog/{item_id}"
    elif platform == "reddit":
        conn_row = get_connection("reddit")
        if not conn_row:
            raise PublishError("Reddit isn't connected yet -- add credentials in the Connections section first.")
        url = _publish_to_reddit(item, conn_row["credentials"])
    elif platform in PLATFORM_FIELDS:
        note = PLATFORM_SETUP_NOTES.get(platform, "")
        raise PublishError(f"{platform} posting isn't wired up yet. {note}")
    else:
        raise ValueError(f"unknown publish platform '{platform}'")

    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        conn.execute(
            "UPDATE growth_content SET status = 'published', published_at = ?, published_platform = ?, published_url = ? WHERE id = ?",
            (now, platform, url, int(item_id)),
        )
        conn.commit()
        updated = conn.execute("SELECT * FROM growth_content WHERE id = ?", (int(item_id),)).fetchone()
    return _decode_flags(dict(updated))


def list_published_blog_posts(limit: int = 50) -> List[Dict[str, Any]]:
    """Public feed -- seo_post items explicitly published to platform='blog'."""
    ensure_growth_schema()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT id, topic, body, published_at FROM growth_content "
            "WHERE status = 'published' AND published_platform = 'blog' AND content_type = 'seo_post' "
            "ORDER BY published_at DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    return [dict(r) for r in rows]
