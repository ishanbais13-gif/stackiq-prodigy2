# app.py
from fastapi import FastAPI, HTTPException
from data_fetcher import fetch_quote, FinnhubError

app = FastAPI(title="baseline", version="1.0")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        return fetch_quote(symbol)
    except FinnhubError as e:
        raise HTTPException(status_code=500, detail=str(e))


















