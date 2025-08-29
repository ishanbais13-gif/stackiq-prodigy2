from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from data_fetcher import get_quote, get_summary, get_history

app = FastAPI(title="StackIQ", version="1.0.0")

# Allow frontend (React in /web) to talk to backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # in production, replace with your domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check
@app.get("/health")
async def health():
    return {"status": "ok"}

# Version info
@app.get("/version")
async def version():
    return {"app": "stackiq-web", "version": "1.0.0"}

# Live stock quote
@app.get("/quote/{symbol}")
async def quote(symbol: str):
    try:
        data = get_quote(symbol)
        return JSONResponse(content=data)
    except Exception as e:
        return {"error": str(e)}

# Stock summary
@app.get("/summary/{symbol}")
async def summary(symbol: str):
    try:
        data = get_summary(symbol)
        return JSONResponse(content=data)
    except Exception as e:
        return {"error": str(e)}

# Historical prices
@app.get("/history/{symbol}")
async def history(symbol: str, range: str = "3m"):
    try:
        data = get_history(symbol, range)
        return JSONResponse(content=data)
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

























