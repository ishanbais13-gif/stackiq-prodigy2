# app.py
import os
import sys
import json
from typing import Any, Dict

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles

import data_fetcher as df  # uses FINNHUB_API_KEY from env

APP_NAME = "stackiq-web"
APP_VERSION = "1.0.0"

app = FastAPI(title=APP_NAME, version=APP_VERSION)

# Serve your frontend from /web (index.html lives in ./web)
if os.path.isdir("web"):
    app.mount("/web", StaticFiles(directory="web", html=True), name="web")


def _pretty_or_json(content: Dict[str, Any], pretty: bool) -> Response:
    if pretty:
        return Response(
            content=json.dumps(content, indent=2, ensure_ascii=False),
            media_type="application/json",
        )
    return JSONResponse(content=content)


# ---------- Health & version ----------
@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/version")
def version(pretty: bool = Query(False)) -> Response:
    payload = {
        "app": APP_NAME,
        "version": APP_VERSION,
        "python": sys.version.split()[0],
        "finnhub_key_set": bool(os.getenv("FINNHUB_API_KEY")),
    }
    return _pretty_or_json(payload, pretty)


# ---------- Data endpoints ----------
@app.get("/quote/{symbol}")
def quote(symbol: str, pretty: bool = Query(False)) -> Response:
    try:
        payload = df.fetch_quote(symbol)
        return _pretty_or_json(payload, pretty)
    except df.FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"quote error: {e}")


@app.get("/earnings/{symbol}")
def earnings(symbol: str, limit: int = 4, pretty: bool = Query(False)) -> Response:
    try:
        payload = df.fetch_earnings(symbol, limit=limit)
        return _pretty_or_json(payload, pretty)
    except df.FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"earnings error: {e}")


@app.get("/history/{symbol}")
def history(symbol: str, range_days: int = 30, pretty: bool = Query(False)) -> Response:
    try:
        payload = df.fetch_history(symbol, range_days=range_days)
        return _pretty_or_json(payload, pretty)
    except df.FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"history error: {e}")


@app.get("/test/{symbol}")
def test(symbol: str, pretty: bool = Query(False)) -> Response:
    """
    Combined payload the UI expects: { symbol, quote, earnings, history }
    """
    try:
        sym = symbol.strip()
        payload = {
            "symbol": sym.upper(),
            "quote": df.fetch_quote(sym),
            "earnings": df.fetch_earnings(sym, limit=4),
            "history": df.fetch_history(sym, range_days=30),
        }
        return _pretty_or_json(payload, pretty)
    except df.FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"test error: {e}")


# Optional: run locally with `python app.py`
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=True,
    )



        )















