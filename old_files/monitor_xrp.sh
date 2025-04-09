#!/bin/bash

# Ensure script runs from its own directory
SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
cd "$SCRIPT_DIR"

# Create logs directory if it doesn't exist
mkdir -p "$SCRIPT_DIR/logs"

# Ensure log files exist and are writable
touch "$SCRIPT_DIR/logs/monitor.log"
touch "$SCRIPT_DIR/logs/xrp_price_monitor.log"

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S'): $*" | tee -a "$SCRIPT_DIR/logs/monitor.log"
}

# Debug logging
debug_log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S'): DEBUG: $*" >> "$SCRIPT_DIR/logs/monitor.log"
}

# Error logging
error_log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S'): ERROR: $*" | tee -a "$SCRIPT_DIR/logs/monitor.log"
}

# Path to the main script (use full path to avoid any ambiguity)
MAIN_SCRIPT="$SCRIPT_DIR/xrp_price_monitor.py"

log "Starting continuous monitoring script"
log "Script directory: $SCRIPT_DIR"
log "Main script path: $MAIN_SCRIPT"

# Ensure the main script is executable
chmod +x "$MAIN_SCRIPT"

# Run in continuous loop
while true; do
    # Check if program is running
    if ! pgrep -f "python.*xrp_price_monitor.py" > /dev/null; then
        error_log "XRP monitor not running, attempting to start..."
        
        # Try to start the script
        debug_log "Attempting to start script with full path and python3"
        python3 "$MAIN_SCRIPT" >> "$SCRIPT_DIR/logs/xrp_price_monitor.log" 2>&1 &
        
        # Capture the PID of the started process
        STARTED_PID=$!
        
        # Wait a moment to see if the process stays up
        sleep 5
        
        # Verify the process is still running
        if kill -0 $STARTED_PID 2>/dev/null; then
            log "Successfully started XRP monitor with PID $STARTED_PID"
        else
            error_log "Failed to start XRP monitor. Check logs for details."
        fi
    else
        debug_log "XRP monitor is already running"
    fi
    
    # Wait 5 seconds before next check
    sleep 5
done
