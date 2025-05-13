"""
Standalone Bosonic client for trad5 endpoint.
"""
import os
import sys
import time
import signal
import json
import threading
import logging
import websocket
import requests
from datetime import datetime

# Add the root directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

# Trad5 Bosonic credentials
BOSONIC_API_USERNAME = "tv-algo-1-tm5-digtltrust@bosonic.digital"
BOSONIC_API_PASSWORD = "Prod1234$"
BOSONIC_API_KEY = "210d4952-b53c-4024-b749-76fb167f4107"
BOSONIC_API_SECRET = "zL6RZx9BoNSP4NZVNUZpK64E3XLAJqn6XhtY"
BOSONIC_API_CODE = "MmQ2NzZlYTQ2OTM0MzkyNjRhMDNlMjU1"
BOSONIC_API_URL = "https://api.bosonic.digital"
BOSONIC_API_BASE_URL = "https://trad5.bosonic.digital"
BOSONIC_WS_URL = "wss://trad5.bosonic.digital/websocket/trader"
BOSONIC_CUSTODIAN_ID = "DIGTL_TRUST"

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/bosonic_trad5.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('bosonic_trad5_client')

# Global running event
running_event = threading.Event()

class BosonicTrad5Client:
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
        self.symbols_of_interest = ["XRPUSD", "XRPUSDC", "XRPEUR", "XRPBTC", "BTCUSD", "ETHUSD"]
        
        # Create logs directory if it doesn't exist
        os.makedirs("logs", exist_ok=True)
        
    def get_auth_token(self):
        """Get authentication token from Bosonic API."""
        try:
            # Try multiple authentication endpoints
            auth_endpoints = [
                "/oauth2/token",
                "/api/v1/auth/token",
                "/api/auth/login",
                "/api/oauth/token"
            ]
            
            for endpoint in auth_endpoints:
                try:
                    logger.info(f"Trying auth endpoint: {endpoint}")
                    
                    auth_url = f"{BOSONIC_API_BASE_URL}{endpoint}"
                    
                    # Try different request formats
                    if endpoint == "/oauth2/token":
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
                    
                    elif endpoint == "/api/v1/auth/token":
                        headers = {
                            "Content-Type": "application/json",
                            "Authorization": f"Basic {BOSONIC_API_CODE}"
                        }
                        data = {
                            "grant_type": "password",
                            "username": BOSONIC_API_USERNAME,
                            "password": BOSONIC_API_PASSWORD
                        }
                        response = requests.post(auth_url, headers=headers, json=data)
                    
                    elif endpoint == "/api/auth/login":
                        headers = {
                            "Content-Type": "application/json"
                        }
                        data = {
                            "username": BOSONIC_API_USERNAME,
                            "password": BOSONIC_API_PASSWORD,
                            "code": BOSONIC_API_CODE
                        }
                        response = requests.post(auth_url, headers=headers, json=data)
                    
                    else:  # "/api/oauth/token"
                        headers = {
                            "Content-Type": "application/json"
                        }
                        data = {
                            "api_key": BOSONIC_API_KEY,
                            "api_secret": BOSONIC_API_SECRET,
                            "grant_type": "client_credentials"
                        }
                        response = requests.post(auth_url, headers=headers, json=data)
                    
                    # Check response
                    if response.status_code == 200:
                        token_data = response.json()
                        logger.info(f"Successfully obtained auth token using endpoint {endpoint}")
                        
                        # Extract token based on response format
                        token = (token_data.get("access_token") or 
                                token_data.get("token") or 
                                token_data.get("accessToken"))
                        
                        if token:
                            return token
                        else:
                            logger.warning(f"No token found in response: {token_data}")
                    else:
                        logger.warning(f"Auth endpoint {endpoint} failed: {response.status_code} - {response.text}")
                
                except Exception as e:
                    logger.warning(f"Error with auth endpoint {endpoint}: {e}")
            
            # If we get here, all auth attempts failed
            logger.error("All authentication attempts failed")
            return None
                
        except Exception as e:
            logger.error(f"Error getting auth token: {e}")
            return None
    
    def on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        try:
            # Log the raw message for debugging
            logger.debug(f"Raw message: {message}")
            
            # Try to parse JSON
            try:
                msg = json.loads(message)
            except json.JSONDecodeError:
                logger.warning(f"Message is not valid JSON: {message}")
                return
            
            # Log different message types
            if isinstance(msg, dict):
                msg_type = msg.get("type")
                
                if msg_type == "heartbeat":
                    self.last_heartbeat = time.time()
                    # Don't log every heartbeat to avoid clutter
                    return
                    
                elif msg_type == "marketData":
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
                
                elif msg_type == "subscribed":
                    logger.info(f"Subscribed to: {msg.get('data', {}).get('subscription')}")
                
                else:
                    # Log any other message types
                    logger.info(f"Received message: {message}")
            else:
                logger.warning(f"Message is not a dictionary: {message}")
                
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
        
        # Log connection success
        logger.info("Successfully connected to Bosonic WebSocket!")
        
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
            
            # Try different connection methods
            connection_methods = [
                self._connect_bearer,
                self._connect_url_token,
                self._connect_token
            ]
            
            for method in connection_methods:
                try:
                    result = method()
                    if result:
                        return True
                except Exception as e:
                    logger.warning(f"Connection method failed: {e}")
            
            logger.error("All connection methods failed")
            return False
            
        except Exception as e:
            logger.error(f"Error connecting to WebSocket: {e}")
            return False
    
    def _connect_bearer(self):
        """Connect with Bearer token."""
        logger.info("Trying connection with Bearer token")
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
    
    def _connect_url_token(self):
        """Connect with token in URL."""
        logger.info("Trying connection with token in URL")
        modified_url = f"{BOSONIC_WS_URL}?token={self.auth_token}"
        self.ws = websocket.WebSocketApp(
            modified_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        return True
    
    def _connect_token(self):
        """Connect with Token header."""
        logger.info("Trying connection with Token header")
        headers = {
            "Authorization": f"Token {self.auth_token}"
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
    
    def run_websocket(self):
        """Run the WebSocket connection in a loop."""
        while self.running_event.is_set():
            try:
                if self.connect():
                    logger.info("Starting WebSocket connection...")
                    self.ws.run_forever()
                    logger.info("WebSocket run_forever ended")
                
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
    print("Starting Bosonic Trad5 Price Feed Client...")
    print("Press Ctrl+C to stop")
    print("=" * 80)
    
    # First check if we can even reach the Bosonic server
    try:
        response = requests.get(BOSONIC_API_BASE_URL, timeout=5)
        logger.info(f"Bosonic API base URL check: {response.status_code}")
    except Exception as e:
        logger.warning(f"Unable to reach Bosonic API base URL: {e}")
        print(f"Warning: Unable to reach {BOSONIC_API_BASE_URL}")
        print("The server may be down or the URL may be incorrect.")
        print("Continuing anyway...")
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Set the running event
    running_event.set()
    
    try:
        # Create and start the Bosonic client
        bosonic_client = BosonicTrad5Client(running_event)
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
