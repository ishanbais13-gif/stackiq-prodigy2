# app.py — StackIQ minimal backend (Flask)
# Safe to paste over your entire file.

import os
import time
import json
from typing import Any, Dict

from flask import Flask, jsonify, request, Response, send_from_directory
from werkzeug.exceptions import HTTPException
from flask_cors import CORS

# ---- Local module that calls Finnhub ----
import data_fetcher  # do not remove; we call into it below


# ---- App metadata ----
START_TIME = time.time()
APP_VERSION = "0.2.0"  # bump this when you ship changes

# ---- Flask app ----
app = Flask(__name__)
CORS(app)  # allow a frontend to call the API (you can tighten this later)

# ---- Helper: call whatever function your data_fetcher exposes ----
def fetch_from_data_fetcher(ticker: str) -> Dict[str, Any]:
    """
    Be flexible about the function name in data_fetcher.py so this keeps
    working even if you named it slightly differently.
    """
    candidates = (
        "get_stock_data",
        "get_quote_and_earnings",
        "get_price_and_earnings",
        "get_ticker_data",
        "get_ticker",
        "get",
        "fetch",
    )
    for name in candidates:
        func = getattr(data_fetcher, name, None)
        if callable(func):
            return func(ticker)

    # If none of the above exist, fall back to an obvious error
    raise RuntimeError(
        "Couldn't find a callable in data_fetcher.py. "
        "Expected one of: " + ", ".join(candidates)
    )


# =========================
#       STATIC FRONTEND
# =========================

APP_DIR = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = os.path.join(APP_DIR, "web")

@app.get("/")
def send_index():
    """
    Serve the Day-3 frontend (web/index.html) at the root URL.
    """
    return send_from_directory(WEB_DIR, "index.html")

@app.get("/web/<path:filename>")
def send_web_static(filename: str):
    """
    Optional helper so you can serve additional files inside /web later.
    Example: /web/styles.css
    """
    return send_from_directory(WEB_DIR, filename)


# =========================
#       API ROUTES
# =========================

@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "stackiq-web"})

@app.get("/version")
def version():
    return jsonify({"version": APP_VERSION})

@app.get("/status")
def status():
    """Operational status + uptime."""
    uptime_seconds = int(time.time() - START_TIME)
    return jsonify({
        "status": "ok",
        "app": "StackIQ",
        "version": APP_VERSION,
        "uptime_seconds": uptime_seconds,
    })

@app.get("/envcheck")
def envcheck():
    """
    Quick check that your Finnhub key is present in the environment.
    """
    has_key = bool(os.environ.get("FINNHUB_API_KEY"))
    return jsonify({"has_key": has_key})

@app.get("/test/<ticker>")
def test_ticker(ticker: str):
    """
    Hit this in your browser:
    /test/AAPL?pretty=1
    /test/MSFT?pretty=1
    """
    ticker = (ticker or "").strip().upper()

    # basic validation
    if not ticker.isalnum() or len(ticker) > 10:
        return jsonify({"error": "Invalid ticker format", "status": "bad_request"}), 400

    try:
        data = fetch_from_data_fetcher(ticker)
        # Optional pretty output via ?pretty=1
        if request.args.get("pretty"):
            return Response(
                json.dumps(data, indent=2),
                mimetype="application/json"
            )
        return jsonify(data)
    except HTTPException as he:  # let Flask handle standard HTTP errors
        raise he
    except Exception as e:
        # Map common "not found" shapes to 404
        msg = str(e).lower()
        if "not found" in msg or "no data" in msg or "404" in msg:
            return jsonify({"error": "Ticker not found or no data", "status": "not_found"}), 404
        # Otherwise 500
        return jsonify({"error": "Internal Server Error", "detail": str(e)}), 500


# =========================
#    SECURITY + ERRORS
# =========================

@app.after_request
def add_headers(resp: Response):
    # Simple safe defaults; tune as needed
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "no-referrer"
    return resp

@app.errorhandler(Exception)
def handle_err(e: Exception):
    if isinstance(e, HTTPException):
        # Friendly JSON for HTTP errors (404 etc.)
        return jsonify(error=e.name, status=e.code), e.code
    return jsonify(error="Internal Server Error", status=500), 500


# =========================
#     LOCAL DEV (only)
# =========================

if __name__ == "__main__":
    # Azure uses gunicorn; this block is ignored on Azure.
    app.run(host="0.0.0.0", port=8000, debug=True)


