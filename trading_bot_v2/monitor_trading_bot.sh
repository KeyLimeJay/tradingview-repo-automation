#monitor_trading_bot.sh
#!/bin/bash

# Change to the application directory
cd /opt/otcxn/trading_bot_v2

# Check if in maintenance mode
if [ -f ".maintenance_mode" ]; then
    echo "$(date): Bot in maintenance mode, not restarting" >> logs/monitor.log
    exit 0
fi

# Check if the bot is running
if ! pgrep -f "python.*main.py.*config/config.json" > /dev/null; then
    echo "$(date): Trading bot is not running. Restarting..." >> logs/monitor.log
    ./start_trading_bot.sh
else
    echo "$(date): Trading bot is running normally" >> logs/monitor.log
fi