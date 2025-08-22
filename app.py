# app.py — StackIQ minimal backend (Flask)
# Safe to paste over your entire file.

import time
import os
import json

from flask import Flask, jsonify, request, Response, send_from_directory
from werkzeug.exceptions import HTTPException
from flask_cors import CORS

import data_fetcher  # local module that calls Finnhub

# ---- App metadata ----
START_TIME = time.time()
APP_VERSION = "0.2.0"  # bump this when you ship changes

# ---- Flask app ----
app = Flask(__name__, static_folder=None)  # we'll serve our own static directory
CORS(app)  # allow frontend to call the API

# -------------------------
# Frontend (serve /web UI)
# -------------------------
WEB_DIR = os.path.join(os.path.dirname(__file__), "web")

@app.get("/web")
def web_index():
    # /web -> serve web/index.html
    return send_from_directory(WEB_DIR, "index.html")

@app.get("/web/<path:filename>")
def web_assets(filename):
    # /web/* -> serve any asset under web/
    return send_from_directory(WEB_DIR, filename)

# -------------------------
# Helper: pretty JSON
# -------------------------
def _maybe_pretty(data):
    if request.args.get("pretty") == "1":
        return Response(json.dumps(data, indent=2), mimetype="application/json")
    return jsonify(data)

# -------------------------
# API routes
# -------------------------
@app.get("/")
def root():
    return "StackIQ backend is live."

@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "stackiq-web"})

@app.get("/version")
def version():
    return jsonify({"version": APP_VERSION})

@app.get("/status")
def status():
    """operational status + uptime."""
    uptime_seconds = int(time.time() - START_TIME)
    return jsonify({
        "status": "ok",
        "app": "StackIQ",
        "version": APP_VERSION,
        "uptime_seconds": uptime_seconds,
    })

@app.get("/envcheck")
def envcheck():
    has_key = bool(os.environ.get("FINNHUB_API_KEY"))
    return jsonify({"has_key": has_key})

# ---------- stock data endpoints ----------
@app.get("/quote/<ticker>")
def quote_only(ticker: str):
    """Just price/quote for a ticker."""
    try:
        data = data_fetcher.get_stock_data(ticker)
        return _maybe_pretty({"ticker": ticker.upper(), "price": data["price"]})
    except Exception as e:
        return jsonify({"error": str(e), "status": "error"}), 500

@app.get("/earnings/<ticker>")
def earnings_only(ticker: str):
    """Just earnings calendar for a ticker."""
    try:
        earnings = data_fetcher.get_ticker_data(ticker).get("earningsCalendar", [])
        return _maybe_pretty({"ticker": ticker.upper(), "earningsCalendar": earnings})
    except Exception as e:
        return jsonify({"error": str(e), "status": "error"}), 500

@app.get("/test/<ticker>")
def combined_test(ticker: str):
    """Combined quote + earnings (what you’ve been testing)."""
    try:
        price = data_fetcher.get_stock_data(ticker)["price"]
        earnings = data_fetcher.get_ticker_data(ticker).get("earningsCalendar", [])
        return _maybe_pretty({
            "ticker": ticker.upper(),
            "price": price,
            "earnings": {"earningsCalendar": earnings},
        })
    except Exception as e:
        return jsonify({"error": str(e), "status": "error"}), 500

# ---- Security headers (simple, safe defaults) ----
@app.after_request
def add_headers(resp: Response):
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "no-referrer"
    return resp

# ---- Friendly JSON errors ----
@app.errorhandler(HTTPException)
def handle_http_err(e: HTTPException):
    return jsonify(error=e.name, status=e.code), e.code

@app.errorhandler(Exception)
def handle_err(e: Exception):
    if isinstance(e, HTTPException):
        return jsonify(error=e.name, status=e.code), e.code
    return jsonify(error="Internal Server Error", status=500), 500

# ---- Local dev only (Azure uses gunicorn; this block is ignored there) ----
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)



