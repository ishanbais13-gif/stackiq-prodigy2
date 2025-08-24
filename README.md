# StackIQ

Stock analysis API built with FastAPI, deployed on Azure.

## Endpoints
- `/` → Health check
- `/quote/{symbol}` → Get live stock quote
- `/earnings/{symbol}` → Get earnings data

## Local run
uvicorn app:app --reload

