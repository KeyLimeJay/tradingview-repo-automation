#!/bin/bash

# Change to the application directory
cd /opt/otcxn/tradingview-repo-automation/multi-account

# Check if in maintenance mode
if [ -f ".maintenance_mode" ]; then
    echo "$(date): Bot in maintenance mode, not restarting" >> logs/monitor.log
    exit 0
fi

# Check if the bot is running
if ! pgrep -f "python.*main.py" > /dev/null; then
    echo "$(date): Multi-account trading bot is not running. Restarting..." >> logs/monitor.log
    ./start_trading_bot.sh
else
    echo "$(date): Multi-account trading bot is running normally" >> logs/monitor.log
fi