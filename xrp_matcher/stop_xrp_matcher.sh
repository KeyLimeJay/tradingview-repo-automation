#!/bin/bash

# Find the process ID of the running XRP matcher
PID=$(pgrep -f "python.*src/main.py")

if [ -z "$PID" ]; then
    echo "XRP Matcher is not running."
    exit 0
fi

echo "Stopping XRP Matcher (PID: $PID)..."
kill -SIGINT $PID

# Wait for the process to terminate
timeout=10
while kill -0 $PID 2>/dev/null; do
    if [ "$timeout" -le 0 ]; then
        echo "Forcing termination..."
        kill -9 $PID
        break
    fi
    timeout=$((timeout-1))
    sleep 1
done

echo "XRP Matcher stopped."
