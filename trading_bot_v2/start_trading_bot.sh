#!/bin/bash

# Change to the application directory
cd /opt/otcxn/trading_bot_v2

# Create logs directory if it doesn't exist
mkdir -p logs

# Check if the bot is already running
if pgrep -f "python.*main.py.*config/config.json" > /dev/null; then
    echo "$(date): Trading bot is already running" | tee -a logs/trading_bot.log
    exit 0
fi

# If we're manually starting the bot, enable monitoring
if [ -f ".maintenance_mode" ]; then
    echo "$(date): Removing maintenance mode flag" | tee -a logs/trading_bot.log
    rm .maintenance_mode
fi

# Make sure monitor script is executable
if [ ! -x "monitor_trading_bot.sh" ]; then
    echo "$(date): Re-enabling monitoring script" | tee -a logs/trading_bot.log
    chmod +x monitor_trading_bot.sh
fi

# Store current PID
echo $$ > .start_pid

# Start the bot with the config file
python main.py --config config/config.json >> logs/trading_bot.log 2>&1 &
BOT_PID=$!

# Store the bot PID
echo $BOT_PID > .bot_pid

# Log startup
echo "$(date): Trading bot started with PID: $BOT_PID" | tee -a logs/trading_bot.log

# Create the running indicator file
touch .bot_running

# Check if auto position manager is active
if grep -q '"enabled": true' config/config.json; then
    echo "$(date): Auto-position management is enabled" | tee -a logs/trading_bot.log
else
    echo "$(date): Auto-position management is NOT enabled, please check configuration" | tee -a logs/trading_bot.log
fi

echo "$(date): Trading bot fully initialized and running" | tee -a logs/trading_bot.log