#!/usr/bin/env python3
"""
Bosonic WebSocket client for connecting to price feed.
"""
import json
import time
import logging
from threading import Thread
import sys

try:
    import websocket
except ImportError:
    print("Installing websocket-client package...")
    import subprocess
    subprocess.call([sys.executable, "-m", "pip", "install", "websocket-client"])
    import websocket

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('bosonic_client')

class BosonicWebSocketClient:
    def __init__(self):
        # Bosonic WebSocket URL and credentials
        self.ws_url = "wss://trad6.bosonic.digital/websocket/trader"
        self.api_code = "NWY3YTFjODExODUwODQ3N2I3MzUzYjcz"
        self.username = "tv-algo-1@bosonic.digital"
        self.password = "Prod1234$"
        
        # WebSocket connection
        self.ws = None
        self.ws_thread = None
        self.running = False
        
        logger.info("Bosonic WebSocket client initialized")
    
    def on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        logger.info(f"Received message: {message}")
        try:
            data = json.loads(message)
            logger.info(f"Parsed message: {json.dumps(data, indent=2)}")
            
            # Check for price updates
            if 'type' in data and data['type'] == 'price_update':
                logger.info(f"Price update: {data['symbol']} - Bid: {data.get('bid')}, Ask: {data.get('ask')}")
            
        except json.JSONDecodeError:
            logger.error(f"Failed to parse message: {message}")
    
    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        logger.error(f"WebSocket error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close event"""
        logger.info(f"WebSocket closed: {close_status_code} - {close_msg}")
        self.running = False
    
    def on_open(self, ws):
        """Handle WebSocket open event"""
        logger.info("WebSocket connection established")
        
        # Send authentication message
        auth_message = {
            "type": "authenticate",
            "username": self.username,
            "password": self.password,
            "api_code": self.api_code
        }
        ws.send(json.dumps(auth_message))
        logger.info("Authentication message sent")
        
        # Subscribe to market data for BTC/USD and ETH/USD
        subscription = {
            "type": "subscribe",
            "channel": "price_feed",
            "symbols": ["BTC/USD", "ETH/USD"]
        }
        ws.send(json.dumps(subscription))
        logger.info(f"Subscribed to price feed for {subscription['symbols']}")
    
    def start(self):
        """Start WebSocket connection"""
        logger.info(f"Connecting to {self.ws_url}...")
        
        # Enable trace for debugging
        websocket.enableTrace(True)
        
        # Create WebSocket connection
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        # Start WebSocket thread
        self.running = True
        self.ws_thread = Thread(target=self.ws.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()
        
        logger.info("WebSocket client started")
        return self.ws_thread
    
    def stop(self):
        """Stop WebSocket connection"""
        self.running = False
        if self.ws:
            self.ws.close()
        logger.info("WebSocket client stopped")

def main():
    client = BosonicWebSocketClient()
    
    # Start WebSocket client
    client.start()
    
    try:
        logger.info("Listening for price updates. Press Ctrl+C to exit.")
        while client.running:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        client.stop()

if __name__ == "__main__":
    main()
