#!/usr/bin/env python3
import requests
import json
import time
import os
import datetime
import re
import subprocess
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class TradingMonitor:
    def __init__(self):
        self.api_url = "http://localhost:6101"
        self.last_signal_time = {}
        self.signals = []
        self.log_file = "strategy_sequence.log"
        self.bot_log_file = "logs/trading_bot.log"
        self.last_log_position = 0
        
        # Create or clear the log file
        with open(self.log_file, "w") as f:
            f.write(f"=== TRADING STRATEGY SEQUENCE MONITOR - {datetime.datetime.now()} ===\n\n")
            
    def log(self, message):
        """Log a message to console and file"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted = f"{timestamp} | {message}"
        print(formatted)
        
        with open(self.log_file, "a") as f:
            f.write(formatted + "\n")
            
    def get_current_status(self):
        """Get current position and repo status"""
        try:
            response = requests.get(f"{self.api_url}/health")
            if response.status_code != 200:
                self.log("ERROR: Failed to get health status")
                return None
                
            data = response.json()
            return data
            
        except Exception as e:
            self.log(f"ERROR: Connection failed: {e}")
            return None
            
    def format_status_line(self, status_data):
        """Format current status as a single line"""
        if not status_data:
            return "STATUS: Unknown"
            
        position_status = status_data.get("position_status", {})
        repo_status = status_data.get("repo_status", {})
        
        status_parts = []
        for symbol, data in position_status.items():
            position = data.get("position", 0)
            
            # Find repo for this symbol
            base_currency = symbol.split('/')[0]
            repo_symbol = f"{base_currency}/USDC110"
            has_repo = repo_status.get(repo_symbol, False)
            
            status_parts.append(f"{symbol}: {position} (Repo: {'Yes' if has_repo else 'No'})")
            
        return "STATUS: " + " | ".join(status_parts)
    
    def check_trading_bot_logs(self):
        """Check trading bot logs for new signals and trades"""
        try:
            if not os.path.exists(self.bot_log_file):
                return
                
            with open(self.bot_log_file, "r") as f:
                # Move to the last read position
                f.seek(self.last_log_position)
                new_lines = f.readlines()
                self.last_log_position = f.tell()
                
            if not new_lines:
                return
                
            # Process new log lines
            for line in new_lines:
                # Check for signals
                if "Received valid trading signal" in line:
                    match = re.search(r'Received valid trading signal: (\{.*\})', line)
                    if match:
                        try:
                            signal_data = json.loads(match.group(1))
                            self.log(f"SIGNAL: {signal_data.get('message')} for {signal_data.get('symbol')} at price {signal_data.get('price')}")
                        except:
                            self.log(f"SIGNAL DETECTED: {line.strip()}")
                
                # Check for trade steps
                if "Executing step" in line:
                    match = re.search(r'Executing step (\d+): (\w+)', line)
                    if match:
                        step_num = match.group(1)
                        step_type = match.group(2)
                        self.log(f"STEP {step_num}: Executing {step_type}")
                
                # Check for trading results
                if "Trade sequence executed successfully" in line:
                    self.log(f"TRADE: Sequence completed successfully")
                
                # Check for validation errors (rejected signals)
                if "Validation error" in line:
                    match = re.search(r'Validation error: (.+)', line)
                    if match:
                        error = match.group(1)
                        self.log(f"REJECTED: {error}")
                
                # Check for skipped trades
                if "No trade steps to execute" in line:
                    self.log(f"SKIPPED: No trade steps to execute")
                    
        except Exception as e:
            self.log(f"ERROR: Failed to check bot logs: {e}")
            
    def simulate_signal(self, symbol, signal_type, price):
        """Simulate sending a signal and monitor response"""
        signal_data = {
            "symbol": symbol,
            "message": signal_type,
            "price": price
        }
        
        self.log(f"SIGNAL: {signal_type} for {symbol} at price {price}")
        
        try:
            response = requests.post(
                f"{self.api_url}/webhook",
                json=signal_data,
                headers={"Content-Type": "application/json"}
            )
            
            result = None
            try:
                result = response.json()
            except:
                self.log(f"ERROR: Could not parse response as JSON: {response.text}")
                return
                
            if response.status_code != 200:
                error = result.get("error", "Unknown error")
                self.log(f"REJECTED: {error}")
                return
                
            # Check for success with no orders
            if result.get("success") and not result.get("orders"):
                message = result.get("message", "No action needed")
                self.log(f"SKIPPED: {message}")
                return
                
            # Process successful trades
            if result.get("success") and result.get("orders"):
                orders = result.get("orders", [])
                for i, order in enumerate(orders):
                    step = order.get("step", "unknown")
                    
                    if "error" in order:
                        self.log(f"FAILED STEP {i+1}: {step} - {order['error']}")
                    elif order.get("skipped", False):
                        self.log(f"SKIPPED STEP {i+1}: {step} - {order.get('reason', 'No reason')}")
                    else:
                        self.log(f"EXECUTED STEP {i+1}: {step}")
                
                # Log final position
                final_position = result.get("final_position", "Unknown")
                self.log(f"RESULT: Final position: {final_position}")
                
        except Exception as e:
            self.log(f"ERROR: Failed to send signal: {e}")
    
    def tail_log(self):
        """Use the tail command to get the most recent line of the trading bot log"""
        try:
            cmd = f"tail -n 1 {self.bot_log_file}"
            output = subprocess.check_output(cmd, shell=True)
            last_line = output.decode('utf-8').strip()
            return last_line
        except Exception as e:
            return None
            
    def monitor_sequence(self):
        """Continuously monitor and display the trading sequence"""
        last_check = time.time()
        check_interval = 10  # seconds
        
        # Initialize last log position
        try:
            if os.path.exists(self.bot_log_file):
                with open(self.bot_log_file, "r") as f:
                    f.seek(0, os.SEEK_END)
                    self.last_log_position = f.tell()
        except:
            pass
            
        try:
            self.log("Starting trading sequence monitor...")
            self.log(f"Results will be logged to {self.log_file}")
            self.log("=" * 80)
            
            # Show recent trading bot activity
            last_line = self.tail_log()
            if last_line:
                self.log(f"Last trading bot activity: {last_line}")
            
            while True:
                current_time = time.time()
                
                # Check for new signals and trades in logs
                self.check_trading_bot_logs()
                
                # Check status periodically
                if current_time - last_check >= check_interval:
                    status = self.get_current_status()
                    status_line = self.format_status_line(status)
                    self.log(status_line)
                    last_check = current_time
                
                # Wait before next check
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.log("Monitor stopped by user")
        except Exception as e:
            self.log(f"Monitor error: {e}")
            
    def run(self):
        """Run the monitor"""
        self.monitor_sequence()

if __name__ == "__main__":
    monitor = TradingMonitor()
    monitor.run()