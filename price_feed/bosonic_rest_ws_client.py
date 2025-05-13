#!/usr/bin/env python3
"""
Bosonic API client using REST API and WebSocket for price data.
"""
import requests
import json
import time
import sys
import datetime
from threading import Thread
try:
    import websocket
except ImportError:
    print("Error: websocket-client package is not installed.")
    print("Please install it with: pip install websocket-client")
    sys.exit(1)

class BosonicClient:
    def __init__(self):
        # Bosonic API credentials
        self.username = "tv-algo-1@bosonic.digital"
        self.password = "Prod1234$"
        self.api_code = "NWY3YTFjODExODUwODQ3N2I3MzUzYjcz"
        self.base_url = "https://trad6.bosonic.digital"
        self.ws_url = "wss://trad6.bosonic.digital/websocket/trader"
        
        # Session and auth token
        self.session = requests.Session()
        self.auth_token = None
        self.ws = None
        self.ws_thread = None
        self.running = False
        
        print("Bosonic client initialized with REST API and WebSocket support")
    
    def login(self):
        """Login to Bosonic API and get auth token"""
        print(f"Logging in to Bosonic API as {self.username}...")
        
        url = f"{self.base_url}/login"
        payload = {
            "username": self.username,
            "password": self.password,
            "code": self.api_code
        }
        
        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            
            # Get auth token from response
            data = response.json()
            self.auth_token = data.get('token')
            
            if self.auth_token:
                print("Login successful, auth token received")
                # Set authorization header for future requests
                self.session.headers.update({
                    "Authorization": f"Bearer {self.auth_token}"
                })
                return True
            else:
                print("Error: No auth token in response")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"Login failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response: {e.response.text}")
            return False
    
    def get_account_info(self):
        """Get account information"""
        if not self.auth_token:
            print("Error: Not logged in")
            return None
        
        try:
            url = f"{self.base_url}/trader/me"
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Failed to get account info: {e}")
            return None
    
    def get_markets(self):
        """Get available markets"""
        if not self.auth_token:
            print("Error: Not logged in")
            return None
        
        try:
            url = f"{self.base_url}/markets"
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Failed to get markets: {e}")
            return None
    
    def get_price_data(self, symbol):
        """Get current price data for a symbol"""
        if not self.auth_token:
            print("Error: Not logged in")
            return None
        
        try:
            url = f"{self.base_url}/market-data/{symbol}"
            response = self.session.get(url)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Failed to get price data for {symbol}: {e}")
            return None
    
    def on_ws_message(self, ws, message):
        """Handle WebSocket messages"""
        try:
            data = json.loads(message)
            print(f"WebSocket message: {json.dumps(data, indent=2)}")
        except json.JSONDecodeError:
            print(f"Received non-JSON message: {message}")
    
    def on_ws_error(self, ws, error):
        """Handle WebSocket errors"""
        print(f"WebSocket error: {error}")
    
    def on_ws_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close"""
        print(f"WebSocket closed: {close_status_code} - {close_msg}")
        self.running = False
    
    def on_ws_open(self, ws):
        """Handle WebSocket open"""
        print("WebSocket connection established")
        
        # Subscribe to market data
        subscription = {
            "type": "subscribe",
            "channel": "market_data",
            "symbols": ["BTC/USD", "ETH/USD"]  # Add your desired symbols
        }
        ws.send(json.dumps(subscription))
        print(f"Subscribed to market data for {subscription['symbols']}")
        
        # Send authentication message
        auth_msg = {
            "type": "authenticate",
            "token": self.auth_token
        }
        ws.send(json.dumps(auth_msg))
        print("Authentication message sent")
    
    def start_websocket(self):
        """Start WebSocket connection"""
        if not self.auth_token:
            print("Error: Not logged in")
            return False
        
        # Set up WebSocket
        websocket.enableTrace(True)  # For debugging
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_open=self.on_ws_open,
            on_message=self.on_ws_message,
            on_error=self.on_ws_error,
            on_close=self.on_ws_close
        )
        
        self.running = True
        self.ws_thread = Thread(target=self.ws.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()
        
        print("WebSocket thread started")
        return True
    
    def stop(self):
        """Stop the client"""
        self.running = False
        if self.ws:
            self.ws.close()
        print("Bosonic client stopped")

def main():
    client = BosonicClient()
    
    # Login
    if not client.login():
        print("Login failed, exiting")
        return
    
    # Get account info
    account_info = client.get_account_info()
    if account_info:
        print(f"Account Info: {json.dumps(account_info, indent=2)}")
    
    # Get available markets
    markets = client.get_markets()
    if markets:
        print(f"Available Markets: {json.dumps(markets, indent=2)}")
    
    # Get price data for a few symbols
    for symbol in ["BTC/USD", "ETH/USD"]:
        price_data = client.get_price_data(symbol)
        if price_data:
            print(f"Price data for {symbol}: {json.dumps(price_data, indent=2)}")
    
    # Start WebSocket for real-time price data
    client.start_websocket()
    
    # Keep the script running
    try:
        print("\nListening for price updates. Press Ctrl+C to exit.")
        while client.running:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        client.stop()

if __name__ == "__main__":
    main()
