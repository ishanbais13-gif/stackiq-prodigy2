# app.py — StackIQ minimal backend (Flask)
# Safe to paste over your entire file.

import time
import os
import json
from flask import Flask, jsonify, request, Response
from werkzeug.exceptions import HTTPException
from flask_cors import CORS

import data_fetcher  # local module that calls Finnhub

# ---- App metadata ----
START_TIME = time.time()
APP_VERSION = "0.2.0"  # bump this when you ship changes

# ---- Flask app ----
app = Flask(__name__)
CORS(app)  # allow frontend to call the API

# ---- helpers ----
def _json(data):
    """Pretty JSON when `?pretty=1` is present."""
    if request.args.get("pretty") == "1":
        return Response(json.dumps(data, indent=2), mimetype="application/json")
    return jsonify(data)

def _ticker_param(t):
    return (t or "").strip().upper()

# ---- Routes ----
@app.get("/")
def root():
    return "StackIQ backend is live."

# Health & version
@app.get("/health")
def health():
    return _json({"ok": True, "service": "stackiq-web"})

@app.get("/version")
def version():
    return _json({"version": APP_VERSION})

# Env check (debug)
@app.get("/envcheck")
def envcheck():
    return _json({"has_key": bool(os.environ.get("FINNHUB_API_KEY"))})

# Operational status + uptime
@app.get("/status")
def status():
    uptime_seconds = int(time.time() - START_TIME)
    return _json({
        "status": "ok",
        "app": "StackIQ",
        "version": APP_VERSION,
        "uptime_seconds": uptime_seconds
    })

# ---- Quotes ----
# Backward compatible old route
@app.get("/test/<ticker>")
def test_ticker(ticker):
    t = _ticker_param(ticker)
    try:
        quote = data_fetcher.get_quote(t)
        earnings = data_fetcher.get_next_earnings(t)
        payload = {"ticker": t, "price": quote, "earnings": earnings}
        return _json(payload)
    except data_fetcher.FetchError as e:
        return _json({"error": e.public_message, "status": e.status}), e.http_code
    except Exception as e:
        return _json({"error": "Internal Server Error"}), 500

# Clean alias (use this going forward)
@app.get("/quote/<ticker>")
def quote_route(ticker):
    t = _ticker_param(ticker)
    try:
        quote = data_fetcher.get_quote(t)
        return _json({"ticker": t, "price": quote})
    except data_fetcher.FetchError as e:
        return _json({"error": e.public_message, "status": e.status}), e.http_code
    except Exception:
        return _json({"error": "Internal Server Error"}), 500

# ---- Simple metrics / analyzer ----
@app.get("/metrics/<ticker>")
def metrics_route(ticker):
    """
    Day-3 starter metrics:
    - change_pct from quote.dp
    - trend label (bullish/neutral/bearish)
    - risk band (based on intraday range vs. price)
    """
    t = _ticker_param(ticker)
    try:
        q = data_fetcher.get_quote(t)
        change_pct = q.get("dp")
        last = q.get("c")
        high = q.get("h")
        low = q.get("l")

        # Trend label
        if change_pct is None:
            trend = "unknown"
        elif change_pct >= 2:
            trend = "bullish"
        elif change_pct <= -2:
            trend = "bearish"
        else:
            trend = "neutral"

        # Simple intraday risk proxy
        risk = "unknown"
        if all(v is not None for v in [last, high, low]) and last:
            intraday_span = abs(high - low)
            span_ratio = intraday_span / max(last, 1e-9)
            if span_ratio >= 0.05:
                risk = "high"
            elif span_ratio >= 0.025:
                risk = "medium"
            else:
                risk = "low"

        data = {
            "ticker": t,
            "last": last,
            "change_pct": change_pct,
            "trend": trend,
            "risk": risk,
        }
        return _json(data)
    except data_fetcher.FetchError as e:
        return _json({"error": e.public_message, "status": e.status}), e.http_code
    except Exception:
        return _json({"error": "Internal Server Error"}), 500

# ---- Recommendations (stub you can grow) ----
@app.get("/recommendation")
def recommendation():
    """
    Starter stub. You can extend this to:
    - pull a watchlist and score each ticker
    - rank by change_pct or custom factors
    """
    return _json({
        "message": "Recommendation engine stub. Add your logic in app.py:/recommendation and/or data_fetcher.py.",
        "how_to_extend": [
            "Call get_quote on your watchlist and sort by change_pct.",
            "Add moving averages via Finnhub candles and rank momentum.",
            "Cache results for 30–60s to avoid rate limits."
        ]
    })

# ---- Security headers (simple, safe defaults) ----
@app.after_request
def add_headers(resp: Response):
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "no-referrer"
    return resp

# ---- Friendly JSON errors ----
@app.errorhandler(Exception)
def handle_err(e: Exception):
    if isinstance(e, HTTPException):
        return _json({"error": e.name, "status": e.code}), e.code
    return _json({"error": "Internal Server Error"}), 500

# ---- Local dev only (Azure uses gunicorn; this block is ignored there) ----
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)

