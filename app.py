# app.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any
import uvicorn

# Import helpers from your data_fetcher.py
from data_fetcher import get_quote, get_earnings

# Create FastAPI app
app = FastAPI(title="StackIQ", version="1.0.0")

# Enable CORS (allowing all origins; tighten if needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check
@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}

# Version endpoint
@app.get("/version")
def version() -> Dict[str, str]:
    return {"version": "1.0.0"}

# Quote endpoint
@app.get("/quote/{symbol}")
def quote(symbol: str) -> Dict[str, Any]:
    try:
        data = get_quote(symbol.upper())
        if not data:
            raise HTTPException(status_code=404, detail="Quote not found")
        return {"symbol": symbol.upper(), "quote": data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Earnings endpoint
@app.get("/earnings/{symbol}")
def earnings(symbol: str) -> Dict[str, Any]:
    try:
        items = get_earnings(symbol.upper())
        return {
            "symbol": symbol.upper(),
            "earnings": {"count": len(items), "items": items},
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Local dev runner (not used on Azure)
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)


        )















