#main.py
"""
Main application for XRP Execution-to-Bid Matcher.
"""
import os
import sys
import time
import signal
import threading
import queue
import logging
import importlib

# Add the parent directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Check required dependencies are installed
def check_dependencies():
    required_packages = {
        'requests': 'requests',
        'websocket-client': 'websocket',
        'python-dotenv': 'dotenv',
        'websockets': 'websockets'
    }
    
    missing_packages = []
    
    for package, module in required_packages.items():
        try:
            if module == 'dotenv':
                importlib.import_module('dotenv')
            else:
                importlib.import_module(module)
            print(f"✓ {package} is installed")
        except ImportError:
            print(f"✗ {package} is NOT installed")
            missing_packages.append(package)
    
    if missing_packages:
        print("\nMissing required packages. Please install them with:")
        print(f"pip install {' '.join(missing_packages)}")
        return False
    
    return True

# Only proceed if all dependencies are installed
if not check_dependencies():
    sys.exit(1)

# Now import our application modules
from config.config import OUTPUT_DIR
from src.utils.logging_setup import setup_logging
from src.websockets.bitstamp_client import BitstampClient
from src.websockets.bosonic_client import BosonicClient
from src.matcher import Matcher

# Global running event
running_event = threading.Event()

def signal_handler(sig, frame):
    """Handle interrupt signals for graceful shutdown."""
    global running_event
    print("\nShutting down XRP Matcher...")
    running_event.clear()
    time.sleep(2)  # Give time for threads to finish
    sys.exit(0)

def main():
    """Main function to run the XRP matcher."""
    global running_event
    
    # Setup logging
    logger = setup_logging()
    
    print("=" * 80)
    print("Starting XRP Execution-to-Bid Matcher...")
    print("Press Ctrl+C to stop")
    print("=" * 80)
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Set the running event
    running_event.set()
    
    try:
        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Create queues for communication between components
        execution_queue = queue.Queue()
        bid_queue = queue.Queue()
        
        # Create and start the Bitstamp client
        bitstamp_client = BitstampClient(execution_queue, running_event)
        bitstamp_ws, bitstamp_thread = bitstamp_client.start()
        logger.info("Bitstamp client started")
        
        # Create and start the Bosonic client
        bosonic_client = BosonicClient(bid_queue, running_event)
        bosonic_thread = bosonic_client.start()
        logger.info("Bosonic client started")
        
        # Create and start the matcher
        matcher = Matcher(execution_queue, bid_queue, running_event)
        matcher_thread = matcher.start()
        logger.info("Matcher started")
        
        # Keep the main thread running
        while running_event.is_set():
            time.sleep(1)
            
        # Shutdown components
        logger.info("Shutting down components...")
        bitstamp_client.stop()
        bosonic_client.stop()
        matcher.stop()
            
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        running_event.clear()
        sys.exit(1)

if __name__ == "__main__":
    main()