#stop_trading_bot.sh
#!/bin/bash

# Change to the application directory
cd /opt/otcxn/trading_bot_v2

# Define log file
LOG_FILE="logs/stop_bot.log"
REPORT_FILE="logs/bot_shutdown_report.log"
MAINTENANCE_FILE=".maintenance_mode"

# Create logs directory if it doesn't exist
mkdir -p logs

# Parse command line arguments
SHOW_HELP=false

for arg in "$@"
do
    case $arg in
        -h|--help)
        SHOW_HELP=true
        shift
        ;;
    esac
done

# Show help information if requested
if [ "$SHOW_HELP" = true ]; then
    echo "Usage: ./stop_trading_bot.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help           Show this help message"
    echo ""
    echo "Description:"
    echo "  This script stops the trading bot and disables monitoring."
    echo "  The monitoring will be automatically re-enabled when you"
    echo "  run the start_trading_bot.sh script."
    exit 0
fi

# Always disable monitoring when stopping the bot
touch "$MAINTENANCE_FILE"
chmod -x monitor_trading_bot.sh

# Initialize status variables
STATUS="SUCCESS"
DETAILS=()

echo "$(date): Attempting to stop trading bot..." | tee -a $LOG_FILE $REPORT_FILE

# Function to check if the bot is running
check_running() {
    if pgrep -f "python.*main.py.*config/config.json" > /dev/null; then
        return 0 # Running
    else
        return 1 # Not running
    fi
}

# Function to get Python processes related to the bot
get_bot_processes() {
    ps aux | grep "python.*main.py.*config/config.json" | grep -v grep
}

# Function to get all trading-related processes
get_all_trading_processes() {
    ps aux | grep -i "trading_bot_v2" | grep -v grep | grep -v "stop_trading_bot"
}

# Check if PID file exists
if [ -f ".bot_pid" ]; then
    BOT_PID=$(cat .bot_pid)
    echo "Found PID file with PID: $BOT_PID" | tee -a $LOG_FILE $REPORT_FILE
    
    if ps -p $BOT_PID > /dev/null; then
        echo "Process $BOT_PID is running. Attempting to kill..." | tee -a $LOG_FILE $REPORT_FILE
        kill -15 $BOT_PID 2>/dev/null  # Try graceful shutdown first
        sleep 2
        
        if ps -p $BOT_PID > /dev/null; then
            echo "Process still running. Forcing termination..." | tee -a $LOG_FILE $REPORT_FILE
            kill -9 $BOT_PID 2>/dev/null
            
            if ps -p $BOT_PID > /dev/null; then
                echo "WARNING: Failed to kill process $BOT_PID" | tee -a $LOG_FILE $REPORT_FILE
                STATUS="PARTIAL"
                DETAILS+=("Failed to kill main bot process $BOT_PID")
            else
                echo "Successfully killed process $BOT_PID" | tee -a $LOG_FILE $REPORT_FILE
                DETAILS+=("Killed main bot process $BOT_PID")
            fi
        else
            echo "Successfully terminated process $BOT_PID" | tee -a $LOG_FILE $REPORT_FILE
            DETAILS+=("Terminated main bot process $BOT_PID")
        fi
    else
        echo "Process $BOT_PID no longer exists" | tee -a $LOG_FILE $REPORT_FILE
        DETAILS+=("Process $BOT_PID was not running")
    fi
    
    # Remove PID file
    rm .bot_pid
    echo "Removed PID file" | tee -a $LOG_FILE $REPORT_FILE
else
    echo "No PID file found" | tee -a $LOG_FILE $REPORT_FILE
fi

# Check if bot is still running
if check_running; then
    echo "Bot processes still running. Getting process details..." | tee -a $LOG_FILE $REPORT_FILE
    
    # Log all running bot processes
    BOT_PROCESSES=$(get_bot_processes)
    echo "$BOT_PROCESSES" | tee -a $LOG_FILE $REPORT_FILE
    
    # Get main process IDs
    BOT_PIDS=$(echo "$BOT_PROCESSES" | awk '{print $2}')
    
    # Kill all main bot processes
    for PID in $BOT_PIDS; do
        echo "Killing bot process with PID: $PID" | tee -a $LOG_FILE $REPORT_FILE
        kill -15 $PID 2>/dev/null  # Try graceful shutdown first
    done
    
    sleep 2
    
    # Check if any processes are still running and force kill if necessary
    if check_running; then
        echo "Some processes still running. Forcing termination..." | tee -a $LOG_FILE $REPORT_FILE
        for PID in $BOT_PIDS; do
            if ps -p $PID > /dev/null; then
                echo "Force killing process $PID" | tee -a $LOG_FILE $REPORT_FILE
                kill -9 $PID 2>/dev/null
            fi
        done
        
        # Check if processes are still running
        if check_running; then
            echo "WARNING: Some processes could not be killed. Using pkill..." | tee -a $LOG_FILE $REPORT_FILE
            pkill -9 -f "python.*main.py.*config/config.json"
            STATUS="PARTIAL"
            DETAILS+=("Had to use pkill as a last resort")
        else
            echo "All main bot processes terminated" | tee -a $LOG_FILE $REPORT_FILE
            DETAILS+=("Terminated all main bot processes")
        fi
    else
        echo "All main bot processes terminated successfully" | tee -a $LOG_FILE $REPORT_FILE
        DETAILS+=("Terminated all main bot processes")
    fi
else
    echo "No main bot processes found running" | tee -a $LOG_FILE $REPORT_FILE
    DETAILS+=("No main bot processes were running")
fi

# Look for any other trading-related processes
OTHER_PROCESSES=$(get_all_trading_processes)
if [ -n "$OTHER_PROCESSES" ]; then
    echo "Found other trading-related processes:" | tee -a $LOG_FILE $REPORT_FILE
    echo "$OTHER_PROCESSES" | tee -a $LOG_FILE $REPORT_FILE
    
    # Count the number of processes
    PROCESS_COUNT=$(echo "$OTHER_PROCESSES" | wc -l)
    DETAILS+=("Found $PROCESS_COUNT other trading-related processes")
    
    # Kill them
    echo "Attempting to kill other trading-related processes..." | tee -a $LOG_FILE $REPORT_FILE
    pkill -f "trading_bot_v2"
    
    sleep 1
    
    # Check if they're still running
    REMAINING=$(get_all_trading_processes)
    if [ -n "$REMAINING" ]; then
        echo "Forcing termination of remaining trading-related processes..." | tee -a $LOG_FILE $REPORT_FILE
        pkill -9 -f "trading_bot_v2"
        
        # Final check
        if [ -n "$(get_all_trading_processes)" ]; then
            echo "WARNING: Some trading-related processes could not be killed" | tee -a $LOG_FILE $REPORT_FILE
            echo "You may need to manually check and kill these processes" | tee -a $LOG_FILE $REPORT_FILE
            STATUS="PARTIAL"
            DETAILS+=("Failed to kill all trading-related processes")
        else
            echo "Successfully terminated all trading-related processes" | tee -a $LOG_FILE $REPORT_FILE
            DETAILS+=("Terminated all trading-related processes")
        fi
    else
        echo "Successfully terminated all trading-related processes" | tee -a $LOG_FILE $REPORT_FILE
        DETAILS+=("Terminated all trading-related processes")
    fi
else
    echo "No other trading-related processes found" | tee -a $LOG_FILE $REPORT_FILE
    DETAILS+=("No other trading-related processes were running")
fi

# Clean up any lock files or markers
if [ -f ".bot_running" ]; then
    rm .bot_running
    echo "Removed bot_running marker file" | tee -a $LOG_FILE $REPORT_FILE
    DETAILS+=("Removed .bot_running marker file")
fi

# Generate shutdown verification report
echo "-----------------------------------------" | tee -a $LOG_FILE $REPORT_FILE
echo "SHUTDOWN VERIFICATION REPORT" | tee -a $LOG_FILE $REPORT_FILE
echo "Timestamp: $(date)" | tee -a $LOG_FILE $REPORT_FILE
echo "Status: $STATUS" | tee -a $LOG_FILE $REPORT_FILE
echo "Details:" | tee -a $LOG_FILE $REPORT_FILE

for detail in "${DETAILS[@]}"; do
    echo "  - $detail" | tee -a $LOG_FILE $REPORT_FILE
done

# Check final status of any running Python processes
PYTHON_PROCESSES=$(ps aux | grep python | grep -v grep)
if [ -n "$PYTHON_PROCESSES" ]; then
    echo "NOTE: Other Python processes are still running (may be unrelated):" | tee -a $LOG_FILE $REPORT_FILE
    echo "$PYTHON_PROCESSES" | tee -a $LOG_FILE $REPORT_FILE
else
    echo "No Python processes are running on the system" | tee -a $LOG_FILE $REPORT_FILE
fi

echo "-----------------------------------------" | tee -a $LOG_FILE $REPORT_FILE
echo "Bot shutdown process completed with status: $STATUS" | tee -a $LOG_FILE $REPORT_FILE
echo "Full shutdown report saved to $REPORT_FILE"

# Modify monitor script to respect maintenance mode if needed
if ! grep -q "maintenance_mode" monitor_trading_bot.sh; then
    # Create a backup of the original monitor script
    cp monitor_trading_bot.sh monitor_trading_bot.sh.bak
    
    # Add maintenance mode check to the monitor script
    awk '
    NR==3 {
        print "# Check if in maintenance mode"
        print "if [ -f \".maintenance_mode\" ]; then"
        print "    echo \"$(date): Bot in maintenance mode, not restarting\" >> logs/monitor.log"
        print "    exit 0"
        print "fi"
        print ""
    }
    {print}' monitor_trading_bot.sh.bak > monitor_trading_bot.sh
    
    echo "Modified monitor script to respect maintenance mode" | tee -a $LOG_FILE $REPORT_FILE
    DETAILS+=("Modified monitor script to respect maintenance mode")
fi

echo "Monitoring has been disabled. Bot will not restart automatically." | tee -a $LOG_FILE $REPORT_FILE
echo "Monitoring will be automatically re-enabled when you run start_trading_bot.sh" | tee -a $LOG_FILE $REPORT_FILE
DETAILS+=("Disabled monitoring functionality")

echo "-----------------------------------------" | tee -a $LOG_FILE $REPORT_FILE
echo "FINAL STATUS: $STATUS" | tee -a $LOG_FILE $REPORT_FILE
echo "MONITORING: Disabled (bot will not restart automatically)" | tee -a $LOG_FILE $REPORT_FILE
echo "-----------------------------------------" | tee -a $LOG_FILE $REPORT_FILE

# Return appropriate exit code
if [ "$STATUS" != "SUCCESS" ]; then
    exit 1
fi

exit 0