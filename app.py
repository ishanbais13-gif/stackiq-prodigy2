from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import data_fetcher as df

app = FastAPI(title="StackIQ", version="1.1")

# allow browser JS to call the API (handy while we build)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten later
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/quote/{symbol}")
def quote(symbol: str, pretty: Optional[int] = 0):
    """
    Returns a normalized quote payload from Finnhub.
    """
    try:
        data = df.fetch_quote(symbol)
        return data
    except df.FinnhubError as e:
        # clean, user-facing error
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal error")

@app.get("/summary/{symbol}")
def summary(symbol: str):
    """
    Short natural-language summary built from the quote.
    """
    try:
        q = df.fetch_quote(symbol)
        sym = q.get("symbol", symbol.upper())
        px = q.get("current")
        pct = q.get("percent_change")
        hi  = q.get("high")
        lo  = q.get("low")
        prev = q.get("prev_close")

        # build a tiny readable blurb
        dir_word = "up" if (pct or 0) > 0 else "down" if (pct or 0) < 0 else "flat"
        pct_txt = f"{pct:.2f}%" if pct is not None else "—"
        hi_txt  = f"{hi:.2f}" if hi is not None else "—"
        lo_txt  = f"{lo:.2f}" if lo is not None else "—"
        prev_txt= f"{prev:.2f}" if prev is not None else "—"
        px_txt  = f"{px:.2f}" if px is not None else "—"

        text = (
            f"{sym}: ${px_txt} ({dir_word} {pct_txt} on the day). "
            f"Session range: ${lo_txt}–${hi_txt}. Prev close: ${prev_txt}."
        )
        return {"symbol": sym, "summary": text, "quote": q}
    except df.FinnhubError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception:
        raise HTTPException(status_code=500, detail="Internal error")





















