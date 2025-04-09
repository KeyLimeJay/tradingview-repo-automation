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

# Add the parent directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

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
        bitstamp_client.start()
        logger.info("Bitstamp client started")
        
        # Create and start the Bosonic client
        bosonic_client = BosonicClient(bid_queue, running_event)
        bosonic_client.start()
        logger.info("Bosonic client started")
        
        # Create and start the matcher
        matcher = Matcher(execution_queue, bid_queue, running_event)
        matcher.start()
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
