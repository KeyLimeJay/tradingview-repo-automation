"""
Standalone Bosonic client for fetching price feed.
"""
import os
import sys
import time
import signal
import json
import threading
import queue
import logging
import websocket
import requests
from datetime import datetime
from dotenv import load_dotenv

# Add the root directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

# Load environment variables
load_dotenv()

# Get Bosonic credentials from environment
BOSONIC_API_USERNAME = os.getenv('BOSONIC_API_USERNAME')
BOSONIC_API_PASSWORD = os.getenv('BOSONIC_API_PASSWORD')
BOSONIC_API_CODE = os.getenv('BOSONIC_API_CODE')
BOSONIC_API_BASE_URL = os.getenv('BOSONIC_API_BASE_URL')
BOSONIC_WS_URL = os.getenv('BOSONIC_WS_URL')

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/bosonic_only.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('bosonic_client')

# Global running event
running_event = threading.Event()

class BosonicOnlyClient:
    """Client for connecting to Bosonic WebSocket API for price feed."""
    
    def __init__(self, running_event):
        """
        Initialize the Bosonic client.
        
        Args:
            running_event: Event to signal when to stop
        """
        self.running_event = running_event
        self.ws = None
        self.auth_token = None
        self.ws_thread = None
        self.last_heartbeat = time.time()
        self.symbols_of_interest = ["XRPUSD", "XRPUSDC", "XRPEUR", "XRPBTC"]  # Add more symbols as needed
        
        # Create logs directory if it doesn't exist
        os.makedirs("logs", exist_ok=True)
        
    def get_auth_token(self):
        """Get authentication token from Bosonic API."""
        try:
            logger.info("Getting auth token from Bosonic API...")
            
            auth_url = f"{BOSONIC_API_BASE_URL}/oauth2/token"
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {BOSONIC_API_CODE}"
            }
            data = {
                "grant_type": "password",
                "username": BOSONIC_API_USERNAME,
                "password": BOSONIC_API_PASSWORD
            }
            
            response = requests.post(auth_url, headers=headers, data=data)
            
            if response.status_code == 200:
                token_data = response.json()
                logger.info("Successfully obtained auth token")
                return token_data.get("access_token")
            else:
                logger.error(f"Failed to get auth token: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting auth token: {e}")
            return None
    
    def on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        try:
            msg = json.loads(message)
            
            # Log different message types
            if msg.get("type") == "heartbeat":
                self.last_heartbeat = time.time()
                # Don't log every heartbeat to avoid clutter
                return
                
            elif msg.get("type") == "marketData":
                # Process market data
                data = msg.get("data", {})
                symbol = data.get("symbol")
                
                # Only process symbols we're interested in
                if symbol in self.symbols_of_interest:
                    bid_price = data.get("bidPrice")
                    bid_size = data.get("bidSize")
                    ask_price = data.get("askPrice")
                    ask_size = data.get("askSize")
                    
                    # Create timestamp
                    now = datetime.now()
                    
                    # Print the price data
                    logger.info(f"Market Data - Symbol: {symbol}, Bid: {bid_price} ({bid_size}), Ask: {ask_price} ({ask_size})")
            
            elif msg.get("type") == "subscribed":
                logger.info(f"Subscribed to: {msg.get('data', {}).get('subscription')}")
            
            else:
                # Log any other message types
                logger.debug(f"Received message: {message}")
                
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON message: {message}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def on_error(self, ws, error):
        """Handle WebSocket errors."""
        logger.error(f"WebSocket error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close."""
        logger.info(f"WebSocket connection closed: {close_status_code} - {close_msg}")
    
    def on_open(self, ws):
        """Handle WebSocket connection open."""
        logger.info("WebSocket connection opened")
        
        # Subscribe to market data for our symbols of interest
        for symbol in self.symbols_of_interest:
            subscription = {
                "type": "subscribe",
                "data": {
                    "subscription": "marketData",
                    "symbol": symbol
                }
            }
            ws.send(json.dumps(subscription))
            logger.info(f"Subscribed to market data for {symbol}")
    
    def heartbeat_monitor(self):
        """Monitor heartbeats and reconnect if needed."""
        while self.running_event.is_set():
            time.sleep(5)  # Check every 5 seconds
            
            # If no heartbeat for 30 seconds, reconnect
            if time.time() - self.last_heartbeat > 30 and self.ws is not None:
                logger.warning("No heartbeat received for 30 seconds, reconnecting...")
                self.ws.close()
                time.sleep(1)
                self.connect()
    
    def connect(self):
        """Connect to the Bosonic WebSocket API."""
        try:
            # Get auth token
            self.auth_token = self.get_auth_token()
            if not self.auth_token:
                logger.error("Failed to get auth token, cannot connect")
                return False
            
            # Connect to WebSocket
            headers = {
                "Authorization": f"Bearer {self.auth_token}"
            }
            self.ws = websocket.WebSocketApp(
                BOSONIC_WS_URL,
                header=headers,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error connecting to WebSocket: {e}")
            return False
    
    def run_websocket(self):
        """Run the WebSocket connection in a loop."""
        while self.running_event.is_set():
            try:
                if self.connect():
                    self.ws.run_forever()
                
                # If we get here, the connection was closed
                logger.info("WebSocket connection closed, reconnecting in 5 seconds...")
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"Error in WebSocket loop: {e}")
                time.sleep(5)  # Wait before reconnecting
    
    def start(self):
        """Start the Bosonic client thread."""
        try:
            # Start the WebSocket thread
            self.ws_thread = threading.Thread(target=self.run_websocket)
            self.ws_thread.daemon = True
            self.ws_thread.start()
            
            # Start the heartbeat monitor thread
            self.heartbeat_thread = threading.Thread(target=self.heartbeat_monitor)
            self.heartbeat_thread.daemon = True
            self.heartbeat_thread.start()
            
            return self.ws_thread
            
        except Exception as e:
            logger.error(f"Error starting Bosonic client: {e}")
            return None
    
    def stop(self):
        """Stop the Bosonic client."""
        if self.ws:
            self.ws.close()


def signal_handler(sig, frame):
    """Handle interrupt signals for graceful shutdown."""
    global running_event
    print("\nShutting down Bosonic client...")
    running_event.clear()
    time.sleep(2)  # Give time for threads to finish
    sys.exit(0)

def main():
    """Main function to run the Bosonic client."""
    global running_event
    
    print("=" * 80)
    print("Starting Bosonic Price Feed Client...")
    print("Press Ctrl+C to stop")
    print("=" * 80)
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Set the running event
    running_event.set()
    
    try:
        # Create and start the Bosonic client
        bosonic_client = BosonicOnlyClient(running_event)
        bosonic_thread = bosonic_client.start()
        
        # Keep the main thread running
        while running_event.is_set():
            time.sleep(1)
        
        # Shutdown the client
        bosonic_client.stop()
            
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        running_event.clear()
        sys.exit(1)

if __name__ == "__main__":
    main()
