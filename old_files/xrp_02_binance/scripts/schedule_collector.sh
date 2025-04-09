#!/bin/bash

# This script sets up a cron job to run the XRP data collector daily
# It should be run once to set up the schedule

# Get the current directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON_SCRIPT="$PROJECT_DIR/auto_xrp_collector.py"

# Make the Python script executable
chmod +x "$PYTHON_SCRIPT"

# Check if the Python script exists
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "Error: Python script not found at $PYTHON_SCRIPT"
    exit 1
fi

# Set the time to run the script (default: 1:00 AM)
HOUR=1
MINUTE=0

# Check if cron is installed
if ! command -v crontab &> /dev/null; then
    echo "Error: crontab is not installed. Please install it first."
    exit 1
fi

# Create cron job entry - run daily at specified time
CRON_ENTRY="$MINUTE $HOUR * * * cd $PROJECT_DIR && python3 $PYTHON_SCRIPT >> $PROJECT_DIR/logs/cron_output.log 2>&1"

# Check if the cron job already exists
EXISTING_CRON=$(crontab -l 2>/dev/null | grep -F "$PYTHON_SCRIPT")

if [ -n "$EXISTING_CRON" ]; then
    echo "Cron job already exists for the XRP data collector"
else
    # Add the new cron job
    (crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -
    echo "Cron job added for XRP data collector, running daily at $HOUR:$(printf "%02d" $MINUTE) AM"
fi

echo "XRP data collector is scheduled to run automatically!"
