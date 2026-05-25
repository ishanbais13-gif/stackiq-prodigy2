#!/bin/bash
mkdir -p /app/data

# Find and copy databases if not already in volume
for db in perf_tracker.db stackiq.db auth.db; do
    if [ ! -f /app/data/$db ]; then
        # Search common locations
        for loc in /app/$db /app/app/$db /$db; do
            if [ -f $loc ]; then
                cp $loc /app/data/$db
                echo "Copied $db from $loc to volume"
                break
            fi
        done
    fi
done

# List what's in the volume for debugging
echo "Volume contents: $(ls /app/data/ 2>/dev/null || echo 'empty')"

uvicorn app:app --host 0.0.0.0 --port $PORT
