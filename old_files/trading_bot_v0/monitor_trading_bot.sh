#!/bin/bash

# Change to the application directory
cd /opt/otcxn/tradingview-repo-automation

# Check if the bot is running
if ! pgrep -f "python.*main.py" > /dev/null; then
    echo "$(date): Trading bot is not running. Restarting..." >> logs/monitor.log
    ./start_trading_bot.sh
else
    echo "$(date): Trading bot is running normally" >> logs/monitor.log
fi