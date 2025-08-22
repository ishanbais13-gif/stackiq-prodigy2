# app.py — minimal Flask backend for StackIQ (Day 1–3 final)
import os
import time
import json

from flask import Flask, jsonify, request, Response
from werkzeug.exceptions import HTTPException
from flask_cors import CORS

import data_fetcher  # local module that calls Finnhub

# ---- App metadata ----
START_TIME = time.time()
APP_VERSION = "0.2.0"

# ---- Flask app ----
app = Flask(__name__)
CORS(app)  # allow the frontend to call the API

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

@app.get("/status")
def status():
    """Operational status + uptime."""
    uptime_seconds = int(time.time() - START_TIME)
    return {
        "status": "ok",
        "app": "StackIQ",
        "version": APP_VERSION,
        "uptime_seconds": uptime_seconds,
    }

@app.get("/envcheck")
def envcheck():
    """Quick check that the FINNHUB_API_KEY is set in the environment."""
    has_key = bool(os.environ.get("FINNHUB_API_KEY"))
    return {"has_key": has_key}

@app.get("/test/<ticker>")
def test_ticker(ticker: str):
    """
    Unified test endpoint:
    - Returns latest quote fields (c, d, dp, h, l, o, pc)
    - Includes the most recent earnings calendar item if available
    Add ?pretty=1 to pretty-print.
    """
    try:
        data = data_fetcher.get_price_and_earnings(ticker.upper())
        # pretty-print if requested
        if request.args.get("pretty"):
            return Response(json.dumps(data, indent=2), mimetype="application/json")
        return jsonify(data)
    except data_fetcher.FetchError as fe:
        return jsonify({"error": str(fe), "status": "bad_request"}), 400
    except Exception as e:
        # Bubble HTTP errors cleanly, everything else -> 500
        if isinstance(e, HTTPException):
            return jsonify(error=e.name, status=e.code), e.code
        return jsonify({"detail": "Unexpected server error", "error": "Internal Server Error"}), 500

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


