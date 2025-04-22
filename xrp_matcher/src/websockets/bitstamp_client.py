#bitstamp_client.py
#bitstamp_client.py
"""
Bitstamp WebSocket client for receiving execution data.
"""
import json
import threading
import time
import hmac
import hashlib
import uuid
import datetime
import websocket
import logging
import requests
from config.config import (
    BITSTAMP_API_KEY, 
    BITSTAMP_API_SECRET, 
    BITSTAMP_WS_URL,
    BITSTAMP_SUBACCOUNT_ID
)

logger = logging.getLogger('xrp_matcher.bitstamp')

class BitstampClient:
    """Client for connecting to Bitstamp WebSocket and receiving trade executions."""
    
    def __init__(self, execution_queue, running_event):
        """
        Initialize the Bitstamp client.
        
        Args:
            execution_queue: Queue to store executions
            running_event: Threading event to signal when to stop
        """
        self.execution_queue = execution_queue
        self.running_event = running_event
        self.ws = None
        self.ws_thread = None
        self.heartbeat_thread = None
        self.token_refresh_thread = None
        self.token = None
        self.token_expiry = None
        
        # Log if we're using a subaccount
        if BITSTAMP_SUBACCOUNT_ID:
            logger.info(f"Using Bitstamp subaccount: {BITSTAMP_SUBACCOUNT_ID}")
        else:
            logger.info("Using Bitstamp main account (no subaccount specified)")
    
    def get_auth_headers(self):
        """Generate authentication headers for Bitstamp API."""
        timestamp = str(int(round(time.time() * 1000)))
        nonce = str(uuid.uuid4())
        message = f"BITSTAMP {BITSTAMP_API_KEY}wsswww.bitstamp.netNone{nonce}{timestamp}v2"
        message = message.encode('utf-8')
        signature = hmac.new(BITSTAMP_API_SECRET.encode('utf-8'), msg=message, digestmod=hashlib.sha256).hexdigest()
        
        # Create base headers
        headers = {
            "X-Auth": f"BITSTAMP {BITSTAMP_API_KEY}",
            "X-Auth-Signature": signature,
            "X-Auth-Nonce": nonce,
            "X-Auth-Timestamp": timestamp,
            "X-Auth-Version": "v2"
        }
        
        # Add subaccount ID if specified
        if BITSTAMP_SUBACCOUNT_ID:
            headers["X-Auth-Subaccount-Id"] = BITSTAMP_SUBACCOUNT_ID
        
        return headers
    
    def get_websocket_token(self):
        """Get authentication token for private WebSocket channels."""
        try:
            # Prepare timestamp and nonce for API request
            timestamp = str(int(round(time.time() * 1000)))
            nonce = str(uuid.uuid4())
            
            # Create the message to sign
            content_type = 'application/x-www-form-urlencoded'
            payload_string = ''
            
            message = 'BITSTAMP ' + BITSTAMP_API_KEY + \
                'POST' + \
                'www.bitstamp.net' + \
                '/api/v2/websockets_token/' + \
                '' + \
                content_type + \
                nonce + \
                timestamp + \
                'v2' + \
                payload_string
            message = message.encode('utf-8')
            
            # Sign the message
            signature = hmac.new(BITSTAMP_API_SECRET.encode('utf-8'), 
                                msg=message, 
                                digestmod=hashlib.sha256).hexdigest()
            
            # Set up headers
            headers = {
                'X-Auth': 'BITSTAMP ' + BITSTAMP_API_KEY,
                'X-Auth-Signature': signature,
                'X-Auth-Nonce': nonce,
                'X-Auth-Timestamp': timestamp,
                'X-Auth-Version': 'v2',
                'Content-Type': content_type
            }
            
            # Add subaccount ID if specified
            if BITSTAMP_SUBACCOUNT_ID:
                headers["X-Auth-Subaccount-Id"] = BITSTAMP_SUBACCOUNT_ID
            
            # Make the request to get the token
            response = requests.post(
                'https://www.bitstamp.net/api/v2/websockets_token/',
                headers=headers,
                data=payload_string
            )
            
            # Check if request was successful
            if response.status_code == 200:
                token_data = response.json()
                valid_sec = token_data.get('valid_sec', 60)
                logger.info(f"Successfully retrieved WebSocket token, valid for {valid_sec} seconds")
                
                # Set token expiry time (subtract 10 seconds for safety)
                self.token_expiry = time.time() + valid_sec - 10
                
                return token_data.get('token')
            else:
                logger.error(f"Failed to get WebSocket token. Status code: {response.status_code}, Response: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting Bitstamp WebSocket token: {e}")
            return None
    
    def refresh_token_periodically(self):
        """Periodically refresh the WebSocket token."""
        while self.running_event.is_set():
            try:
                # Check if token needs refreshing
                if self.token_expiry is None or time.time() > self.token_expiry:
                    self.token = self.get_websocket_token()
                    if self.token:
                        # Resubscribe to private channels
                        if self.ws and self.ws.sock and self.ws.sock.connected:
                            self.subscribe_to_private_channels()
                
                # Sleep for a while (check every 30 seconds)
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"Error refreshing Bitstamp token: {e}")
                time.sleep(30)  # Wait a bit before trying again
    
    def subscribe_to_private_channels(self):
        """Subscribe to private WebSocket channels."""
        if not self.token:
            logger.warning("Cannot subscribe to private channels - no token available")
            return
        
        # Subscribe to private trades channel
        private_trades_subscription = {
            "event": "bts:subscribe",
            "data": {
                "channel": "private-my_trades_xrpusd",
                "auth": self.token
            }
        }
        self.ws.send(json.dumps(private_trades_subscription))
        logger.info("Subscribed to private XRPUSD trades channel")
        
        # Subscribe to self-trades channel (optional)
        private_live_trades_subscription = {
            "event": "bts:subscribe",
            "data": {
                "channel": "private-live_trades_xrpusd",
                "auth": self.token
            }
        }
        self.ws.send(json.dumps(private_live_trades_subscription))
        logger.info("Subscribed to private live XRPUSD trades channel")
    
    def on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        try:
            data = json.loads(message)
            
            # Handle private trades from your account
            if 'event' in data and data['event'] == 'trade' and 'channel' in data and 'private-my_trades_xrpusd' in data['channel']:
                trade_data = data['data']
                
                # Format the data for private trades
                microsecond_datetime = datetime.datetime.fromtimestamp(
                    int(trade_data.get('microtimestamp', time.time() * 1000000)) / 1000000)
                
                formatted_trade = {
                    'id': trade_data.get('id'),
                    'datetime': microsecond_datetime,
                    'datetime_str': microsecond_datetime.strftime('%Y-%m-%d %H:%M:%S.%f'),
                    'symbol': 'XRPUSD',
                    'side': trade_data.get('side', 'Buy'),
                    'amount': float(trade_data.get('amount')),
                    'price': float(trade_data.get('price')),
                    'subaccount_id': BITSTAMP_SUBACCOUNT_ID if BITSTAMP_SUBACCOUNT_ID else 'main',
                    'source': 'own_account',
                    'trade_account_id': trade_data.get('trade_account_id', 'unknown')
                }
                
                logger.info(f"Own Execution: ID: {formatted_trade['id']}, "
                          f"DateTime: {formatted_trade['datetime_str']}, "
                          f"Symbol: {formatted_trade['symbol']}, "
                          f"Side: {formatted_trade['side']}, "
                          f"Quantity: {formatted_trade['amount']}, "
                          f"Rate: {formatted_trade['price']}, "
                          f"Account: {formatted_trade['trade_account_id']}")
                
                self.execution_queue.put(formatted_trade)
                
            # Handle self-trades if needed
            elif 'event' in data and data['event'] == 'self_trade' and 'channel' in data and 'private-live_trades_xrpusd' in data['channel']:
                trade_data = data['data']
                
                # Process self-trade events if needed
                logger.debug(f"Received self-trade event: {trade_data}")
                
            # Handle public trades
            elif 'event' in data and data['event'] == 'trade' and 'data' in data and data['channel'] == 'live_trades_xrpusd':
                trade_data = data['data']
                
                # Check if this is a buy trade for XRPUSD
                if int(trade_data['type']) == 0:  # Buy trade
                    
                    # Format the data with microsecond precision
                    timestamp = int(trade_data['timestamp'])
                    microsecond_datetime = datetime.datetime.fromtimestamp(timestamp)
                    
                    # Add microseconds from microtimestamp if available
                    if 'microtimestamp' in trade_data:
                        micros = int(trade_data['microtimestamp']) % 1000000
                        microsecond_datetime = microsecond_datetime.replace(microsecond=micros)
                    
                    formatted_trade = {
                        'id': trade_data['id'],
                        'datetime': microsecond_datetime,
                        'datetime_str': microsecond_datetime.strftime('%Y-%m-%d %H:%M:%S.%f'),
                        'symbol': 'XRPUSD',
                        'side': 'Buy',
                        'amount': float(trade_data['amount']),
                        'price': float(trade_data['price']),
                        'subaccount_id': BITSTAMP_SUBACCOUNT_ID if BITSTAMP_SUBACCOUNT_ID else 'main',
                        'source': 'market',  # Market trade (not from your account)
                        'trade_account_id': 'other'
                    }
                    
                    logger.info(f"Market Execution: ID: {formatted_trade['id']}, "
                              f"DateTime: {formatted_trade['datetime_str']}, "
                              f"Symbol: {formatted_trade['symbol']}, "
                              f"Side: {formatted_trade['side']}, "
                              f"Quantity: {formatted_trade['amount']}, "
                              f"Rate: {formatted_trade['price']}")
                    
                    self.execution_queue.put(formatted_trade)
        
        except json.JSONDecodeError:
            logger.warning(f"Received invalid JSON message from Bitstamp: {message[:100]}...")
        except Exception as e:
            logger.error(f"Error processing Bitstamp message: {e}")
    
    def on_error(self, ws, error):
        """Handle WebSocket errors."""
        logger.error(f"Bitstamp WebSocket Error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection closure."""
        logger.info(f"Bitstamp WebSocket connection closed: {close_status_code} - {close_msg}")
        
        # Only attempt reconnection if the program is still running
        if self.running_event.is_set():
            logger.info("Attempting to reconnect to Bitstamp in 5 seconds...")
            time.sleep(5)
            self.start()
    
    def on_open(self, ws):
        """Handle WebSocket connection opening."""
        logger.info("Bitstamp WebSocket connection established")
        
        # Subscribe to public XRPUSD trades
        public_subscription = {
            "event": "bts:subscribe",
            "data": {
                "channel": "live_trades_xrpusd"
            }
        }
        ws.send(json.dumps(public_subscription))
        logger.info("Subscribed to XRPUSD trades")
        
        # Subscribe to private channels if we have a token
        if self.token:
            self.subscribe_to_private_channels()
        else:
            # Try to get a new token and subscribe
            self.token = self.get_websocket_token()
            if self.token:
                self.subscribe_to_private_channels()
            else:
                logger.warning("Could not subscribe to private channels - unable to get token")
    
    def keep_alive(self):
        """Send periodic heartbeat messages."""
        while self.running_event.is_set():
            try:
                if self.ws is not None and self.ws.sock and self.ws.sock.connected:
                    self.ws.send(json.dumps({"event": "bts:heartbeat"}))
                time.sleep(30)
            except Exception as e:
                logger.error(f"Bitstamp heartbeat error: {e}")
                break
    
    def start(self):
        """Start the WebSocket connection."""
        # First, attempt to get a token for private channels
        self.token = self.get_websocket_token()
        
        # Create WebSocket connection
        self.ws = websocket.WebSocketApp(
            BITSTAMP_WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            header=self.get_auth_headers()
        )
        
        # Start heartbeat thread
        self.heartbeat_thread = threading.Thread(target=self.keep_alive)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()
        
        # Start token refresh thread
        self.token_refresh_thread = threading.Thread(target=self.refresh_token_periodically)
        self.token_refresh_thread.daemon = True
        self.token_refresh_thread.start()
        
        # Run WebSocket in a separate thread
        self.ws_thread = threading.Thread(target=self.ws.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()
        
        return self.ws, self.ws_thread
    
    def stop(self):
        """Stop the WebSocket connection."""
        if self.ws is not None:
            self.ws.close()