from fastapi import FastAPI, HTTPException
import data_fetcher as df

app = FastAPI(title="baseline", version="1.0")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/quote/{symbol}")
def quote(symbol: str):
    try:
        return df.fetch_quote(symbol)
    except df.FinnhubError as e:
        # Upstream/usage issue (bad key, 429, etc.)
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        # Anything unexpected—don’t crash as 500 plain text
        raise HTTPException(status_code=500, detail="upstream_error")




















