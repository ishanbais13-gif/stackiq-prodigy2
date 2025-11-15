# ml/config.py

import os
from pathlib import Path

# --------- API / DATA CONFIG ----------

# Finnhub API key - make sure this is set in your environment:
# FINNHUB_API_KEY=your_key_here
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")

if not FINNHUB_API_KEY:
    # Don't crash, but warn in logs when we start actually running
    print("[WARNING] FINNHUB_API_KEY is not set. Set it in your environment before fetching data.")

# Default symbols we care about for v1
DEFAULT_SYMBOLS = ["NVDA", "AAPL", "SPY", "QQQ", "TSLA"]

# How many years of history to pull for v1 model
YEARS_BACK = 5

# Base directories
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
MODELS_DIR = PROJECT_ROOT / "models"
BACKTESTS_DIR = PROJECT_ROOT / "backtests"
LOGS_DIR = PROJECT_ROOT / "logs"

# Make sure directories exist
for d in [DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, MODELS_DIR, BACKTESTS_DIR, LOGS_DIR]:
    d.mkdir(parents=True, exist_ok=True)
