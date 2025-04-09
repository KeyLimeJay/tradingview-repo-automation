# bosonic_client.py
# bosonic_client.py
"""
Bosonic WebSocket client for receiving bid data.
Uses websocket-client library instead of websockets to avoid compatibility issues.
"""
import json
import threading
import time
import datetime
import logging
import websocket  # websocket-client library
import requests
from config.config import (
    BOSONIC_API_USERNAME, BOSONIC_API_PASSWORD, BOSONIC_API_CODE,
    BOSONIC_API_BASE_URL, BOSONIC_WS_URL
)

logger = logging.getLogger('xrp_matcher.bosonic')

class BosonicClient:
    """Client for connecting to Bosonic WebSocket and receiving bid data."""
    
    def __init__(self, bid_queue, running_event):
        """
        Initialize the Bosonic client.
        
        Args:
            bid_queue: Queue to store bids
            running_event: Threading event to signal when to stop
        """
        self.bid_queue = bid_queue
        self.running_event = running_event
        self.ws = None
        self.ws_thread = None
        self.heartbeat_thread = None
    
    def login(self):
        """Authenticate with the Bosonic API to get a token for WebSocket connection."""
        try:
            session = requests.Session()
            session.headers.update({
                'Content-Type': 'application/json',
                'Accept': '*/*',
                'Origin': BOSONIC_API_BASE_URL,
                'Referer': f'{BOSONIC_API_BASE_URL}/login?noredir=1'
            })
            
            login_data = {
                "username": BOSONIC_API_USERNAME,
                "password": BOSONIC_API_PASSWORD,
                "code": BOSONIC_API_CODE,
                "redirectTo": f"{BOSONIC_API_BASE_URL}/trader"
            }
            
            response = session.post(
                f"{BOSONIC_API_BASE_URL}/sso/api/login",
                json=login_data
            )
            
            if response.status_code == 200:
                token = response.headers.get('Authorization', response.headers.get('authorization'))
                if token and token.startswith('Bearer '):
                    # Store without Bearer prefix for websocket
                    token = token.replace('Bearer ', '')
                    
                logger.info("Bosonic authentication successful")
                return token
            else:
                logger.error(f"Bosonic authentication failed: {response.status_code} - {response.text}")
                return None
                    
        except Exception as e:
            logger.error(f"Bosonic authentication error: {str(e)}")
            return None
    
    def on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        try:
            data = json.loads(message)
            
            message_type = data.get('type', '')
            
            if message_type == 'ticker':
                self.handle_ticker_update(data)
            elif message_type == 'marketdata':
                self.handle_marketdata_update(data)
                
        except json.JSONDecodeError:
            logger.warning(f"Received invalid JSON message from Bosonic: {message[:100]}...")
        except Exception as e:
            logger.error(f"Error handling Bosonic message: {str(e)}")
    
    def handle_ticker_update(self, data):
        """Handle ticker update messages (for price data)."""
        try:
            content = data.get('content', {})
            symbol = content.get('symbol')
            
            # Skip if not our target symbol
            if symbol != "XRP/USDT":
                return
                
            # Extract timestamp
            timestamp = datetime.datetime.now()
            timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
            
            # Extract bid price and size
            bid_price = None
            bid_size = None
            
            # Extract bid price
            for field in ['bid', 'bidPx', 'bidPrice']:
                if field in content:
                    bid_price = float(content.get(field, 0))
                    break
                    
            # Extract bid size
            for field in ['bidSize', 'bidQty', 'bidQuantity']:
                if field in content:
                    bid_size = float(content.get(field, 0))
                    break
            
            # Only process if we have both bid price and size
            if bid_price is not None and bid_size is not None and bid_price > 0 and bid_size > 0:
                bid_data = {
                    'timestamp': timestamp,
                    'timestamp_str': timestamp_str,
                    'symbol': 'XRPUSD',  # Normalize to match execution symbol
                    'bid_price': bid_price,
                    'bid_size': bid_size,
                    'remaining_size': bid_size,  # Track remaining size for matching
                    'source': 'ticker'
                }
                
                logger.debug(f"Bid: Timestamp: {timestamp_str}, "
                           f"Symbol: {bid_data['symbol']}, "
                           f"Price: {bid_price}, "
                           f"Size: {bid_size}")
                
                # Add to queue for processing
                self.bid_queue.put(bid_data)
                
        except Exception as e:
            logger.error(f"Error processing Bosonic ticker update: {str(e)}")
    
    def handle_marketdata_update(self, data):
        """Handle market data messages with detailed bid/offer information."""
        try:
            content = data.get('content', {})
            symbol = content.get('symbol')
            
            # Skip if not our target symbol
            if symbol != "XRP/USDT":
                return
                
            # Get timestamp
            timestamp = datetime.datetime.now()
            timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')
            
            # Extract bids
            bids = content.get('bids', [])
            
            if not bids:
                return
                
            # Process best bid (first in the list)
            bid = bids[0]
            
            # Extract bid price
            bid_price = None
            for field in ['quotePx', 'price', 'px']:
                if field in bid:
                    bid_price = float(bid.get(field, 0))
                    break
            
            # Extract bid size
            bid_size = None
            for field in ['quoteSize', 'size', 'qty', 'quantity']:
                if field in bid:
                    bid_size = float(bid.get(field, 0))
                    break
            
            # Only process if we have both bid price and size
            if bid_price is not None and bid_size is not None and bid_price > 0 and bid_size > 0:
                bid_data = {
                    'timestamp': timestamp,
                    'timestamp_str': timestamp_str,
                    'symbol': 'XRPUSD',  # Normalize to match execution symbol
                    'bid_price': bid_price,
                    'bid_size': bid_size,
                    'remaining_size': bid_size,  # Track remaining size for matching
                    'source': 'marketdata'
                }
                
                logger.debug(f"Bid: Timestamp: {timestamp_str}, "
                           f"Symbol: {bid_data['symbol']}, "
                           f"Price: {bid_price}, "
                           f"Size: {bid_size}")
                
                # Add to queue for processing
                self.bid_queue.put(bid_data)
                        
        except Exception as e:
            logger.error(f"Error processing Bosonic market data update: {str(e)}")
    
    def on_error(self, ws, error):
        """Handle WebSocket errors."""
        logger.error(f"Bosonic WebSocket Error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection closure."""
        logger.info(f"Bosonic WebSocket connection closed: {close_status_code} - {close_msg}")
        
        # Only attempt reconnection if the program is still running
        if self.running_event.is_set():
            logger.info("Attempting to reconnect to Bosonic in 5 seconds...")
            time.sleep(5)
            self.start()
    
    def on_open(self, ws):
        """Handle WebSocket connection opening."""
        logger.info("Bosonic WebSocket connected")
        
        # Subscribe to ticker and marketdata
        subscriptions = [
            {"type": "ticker.subscribe"},
            {"type": "marketdata.subscribe"}
        ]
        
        for sub in subscriptions:
            ws.send(json.dumps(sub))
            logger.debug(f"Sent Bosonic subscription: {sub}")
    
    def keep_alive(self):
        """Send periodic heartbeat messages."""
        while self.running_event.is_set():
            try:
                if self.ws is not None:
                    self.ws.send(json.dumps({"type": "ping"}))
                time.sleep(30)
            except Exception as e:
                logger.error(f"Bosonic heartbeat error: {e}")
                break
    
    def start(self):
        """Start the WebSocket connection."""
        # Get token for authentication
        token = self.login()
        
        if not token:
            logger.error("Failed to obtain Bosonic authentication token, cannot connect to WebSocket")
            # Try again after delay
            if self.running_event.is_set():
                time.sleep(30)
                return None, None
            return None, None
            
        logger.info(f"Connecting to Bosonic WebSocket: {BOSONIC_WS_URL}")
        
        # Authentication headers
        headers = {
            "Origin": BOSONIC_API_BASE_URL,
            "User-Agent": "Mozilla/5.0",
            "Authorization": f"Bearer {token}"
        }
        
        # Create websocket connection
        self.ws = websocket.WebSocketApp(
            BOSONIC_WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            header=headers
        )
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self.keep_alive)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        # Run WebSocket in a separate thread
        self.ws_thread = threading.Thread(target=self.ws.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()
        
        return self.ws, self.ws_thread
    
    def stop(self):
        """Stop the WebSocket connection."""
        if self.ws is not None:
            self.ws.close()