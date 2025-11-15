# app.py - BASE STABLE MODE
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(
    title="StackIQ API",
    version="0.1.0",
    description="Base stable mode – engine not wired in yet."
)

class PredictRequest(BaseModel):
    symbol: str
    budget: float

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "mode": "base",
        "message": "App is running, engine disabled for now."
    }

@app.post("/predict")
async def dummy_predict(req: PredictRequest):
    # TEMPORARY: no real engine here yet
    return {
        "symbol": req.symbol.upper(),
        "budget": req.budget,
        "signal": "HOLD",
        "confidence": 0.0,
        "note": "Engine will be wired up tomorrow."
    }







































































