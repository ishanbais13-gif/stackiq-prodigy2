#!/bin/bash

VENV=/Users/ishanbais/stackiq-prodigy2/.venv
WORKDIR=/Users/ishanbais/stackiq-prodigy2
LOGFILE="$WORKDIR/server.log"

# If something is already bound to port 8000, exit cleanly
if lsof -iTCP:8000 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') Port 8000 already in use — skipping start." >> "$LOGFILE"
    exit 0
fi

source "$VENV/bin/activate"
cd "$WORKDIR"

echo "$(date '+%Y-%m-%d %H:%M:%S') Starting uvicorn (PID $$)..." >> "$LOGFILE"

# exec replaces this shell with uvicorn so launchd tracks the correct PID
exec uvicorn app:app --host 0.0.0.0 --port 8000 >> "$LOGFILE" 2>&1
