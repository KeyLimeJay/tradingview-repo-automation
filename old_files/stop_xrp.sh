#!/bin/bash

# Kill the monitor script
echo "Stopping monitor script..."
MONITOR_PID=$(pgrep -f "bash.*monitor_xrp.sh")
if [ -n "$MONITOR_PID" ]; then
    kill $MONITOR_PID
    echo "Monitor script stopped (PID: $MONITOR_PID)"
else
    echo "Monitor script not running"
fi

# Kill the XRP price monitor
echo "Stopping XRP price monitor..."
XRP_PID=$(pgrep -f "python.*xrp_price_monitor.py")
if [ -n "$XRP_PID" ]; then
    kill $XRP_PID
    echo "XRP price monitor stopped (PID: $XRP_PID)"
else
    echo "XRP price monitor not running"
fi

echo "All processes stopped"
