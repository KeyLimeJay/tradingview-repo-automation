#!/bin/bash

# Change to the application directory
cd /opt/otcxn/tradingview-repo-automation/multi-account

# Check if the bot is already running
if pgrep -f "python.*main.py" > /dev/null; then
    echo "$(date): Multi-account trading bot is already running" | tee -a logs/trading_bot.log
    exit 0
fi

# If we're manually starting the bot, enable monitoring
if [ -f ".maintenance_mode" ]; then
    echo "$(date): Removing maintenance mode flag" | tee -a logs/trading_bot.log
    rm .maintenance_mode
fi

# Make sure monitor script is executable
if [ -f "../monitor_trading_bot.sh" ] && [ ! -x "../monitor_trading_bot.sh" ]; then
    echo "$(date): Re-enabling monitoring script" | tee -a logs/trading_bot.log
    chmod +x ../monitor_trading_bot.sh
fi

# Store current PID
echo $$ > .start_pid

# Start the bot
python main.py >> logs/trading_bot.log 2>&1 &
BOT_PID=$!

# Store the bot PID
echo $BOT_PID > .bot_pid

# Log startup
echo "$(date): Multi-account trading bot started with PID: $BOT_PID" | tee -a logs/trading_bot.log

# Create the running indicator file
touch .bot_running

# Check if strategy monitor exists
if [ -f "../trading_strategy_monitor.py" ]; then
    # Make sure it's executable
    chmod +x ../trading_strategy_monitor.py
    
    # Start the strategy monitor in the background, logging to its own file
    echo "$(date): Starting strategy monitor" | tee -a logs/strategy_monitor.log
    python ../trading_strategy_monitor.py --interval 30 >> logs/strategy_monitor.log 2>&1 &
    MONITOR_PID=$!
    
    # Store the monitor PID
    echo $MONITOR_PID > .monitor_pid
    
    echo "$(date): Strategy monitor started with PID: $MONITOR_PID" | tee -a logs/strategy_monitor.log
else
    echo "$(date): Strategy monitor script not found. Please make sure trading_strategy_monitor.py exists." | tee -a logs/trading_bot.log
fi