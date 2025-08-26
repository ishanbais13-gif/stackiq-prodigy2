from fastapi import FastAPI

app = FastAPI(title="baseline", version="1.0")

@app.get("/health")
def health():
    return {"status": "ok"}

















