# app.py — StackIQ minimal backend (Flask)
# Safe to paste over your entire file.

import time
import os

from flask import Flask, jsonify, request, Response
from werkzeug.exceptions import HTTPException
from flask_cors import CORS

import data_fetcher  # local module that calls Finnhub

# ---- App metadata ----
START_TIME = time.time()
APP_VERSION = "0.1.0"  # bump this when you ship changes

# ---- Flask app ----
app = Flask(__name__)
CORS(app)  # allow frontend to call the API

# ---- Routes ----
@app.get("/")
def root():
    return "StackIQ backend is live."

@app.get("/health")
def health():
    return {"ok": True, "service": "stackiq-web"}

@app.get("/version")
def version():
    return {"version": APP_VERSION}

@app.get("/envcheck")
def envcheck():
    """Quick check that FINNHUB_API_KEY is available to the app."""
    return {"has_key": bool(os.environ.get("FINNHUB_API_KEY"))}

@app.get("/test/<ticker>")
def test_ticker(ticker: str):
    """
    Example data for a ticker.
    Add ?pretty=1 to pretty-print JSON in the browser.
    """
    data = {
        "ticker": ticker.upper(),
        "price": data_fetcher.get_stock_price(ticker),
        "earnings": data_fetcher.get_next_earnings(ticker),
    }

    # Optional pretty JSON for humans in browser
    if request.args.get("pretty") == "1":
        import json
        return Response(
            json.dumps(data, indent=2),
            mimetype="application/json"
        )

    return jsonify(data)

@app.get("/status")
def status():
    """Operational status + uptime."""
    uptime_seconds = int(time.time() - START_TIME)
    return {
        "status": "ok",
        "app": "StackIQ",
        "version": APP_VERSION,
        "uptime_seconds": uptime_seconds
    }

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
        return jsonify(error=e.name, status=e.code), e.code
    return jsonify(error="Internal Server Error", status=500), 500

# ---- Local dev only (Azure uses gunicorn; this block is ignored there) ----
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)



