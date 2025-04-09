#!/bin/bash

# This script sets up a cron job to run the Trade Execution Processor daily
# It reads configuration from config.json

# Get the current directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PYTHON_SCRIPT="$SCRIPT_DIR/process_trades.py"
CONFIG_FILE="$SCRIPT_DIR/config.json"

# Make the Python script executable
chmod +x "$PYTHON_SCRIPT"

# Check if the Python script exists
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "Error: Python script not found at $PYTHON_SCRIPT"
    exit 1
fi

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file not found at $CONFIG_FILE"
    exit 1
fi

# Read scheduling configuration from config.json
ENABLED=$(grep -o '"enabled": *true' "$CONFIG_FILE" | wc -l)
HOUR=$(grep -o '"hour": *[0-9]\+' "$CONFIG_FILE" | awk '{print $2}')
MINUTE=$(grep -o '"minute": *[0-9]\+' "$CONFIG_FILE" | awk '{print $2}')

# Validate values
if [ -z "$HOUR" ]; then HOUR=2; fi
if [ -z "$MINUTE" ]; then MINUTE=30; fi

# Check if scheduling is enabled
if [ "$ENABLED" -eq 0 ]; then
    echo "Scheduling is disabled in the configuration. Exiting."
    exit 0
fi

# Check if cron is installed
if ! command -v crontab &> /dev/null; then
    echo "Error: crontab is not installed. Please install it first."
    exit 1
fi

# Create cron job entry - run daily at specified time
CRON_ENTRY="$MINUTE $HOUR * * * cd $SCRIPT_DIR && python3 $PYTHON_SCRIPT >> $SCRIPT_DIR/logs/cron_output.log 2>&1"

# Check if the cron job already exists
EXISTING_CRON=$(crontab -l 2>/dev/null | grep -F "$PYTHON_SCRIPT")

if [ -n "$EXISTING_CRON" ]; then
    echo "Cron job already exists for the Trade Execution Processor"
else
    # Add the new cron job
    (crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -
    echo "Cron job added for Trade Execution Processor, running daily at $HOUR:$(printf "%02d" $MINUTE)"
fi

echo "Trade Execution Processor is scheduled to run automatically!"
