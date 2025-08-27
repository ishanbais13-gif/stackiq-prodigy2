from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import os

app = FastAPI(title="baseline", version="1.0")

# Health check
@app.get("/health")
def health():
    return {"status": "ok"}

# Serve static web folder (index.html, css, js)
app.mount("/web", StaticFiles(directory="web"), name="web")

# Route root "/" to index.html inside /web
@app.get("/")
async def root():
    index_path = os.path.join("web", "index.html")
    return FileResponse(index_path)






















