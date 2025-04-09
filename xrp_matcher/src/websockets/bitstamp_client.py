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
from config.config import BITSTAMP_API_KEY, BITSTAMP_API_SECRET, BITSTAMP_WS_URL

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
    
    def get_auth_headers(self):
        """Generate authentication headers for Bitstamp API."""
        timestamp = str(int(round(time.time() * 1000)))
        nonce = str(uuid.uuid4())
        message = f"BITSTAMP {BITSTAMP_API_KEY}wsswww.bitstamp.netNone{nonce}{timestamp}v2"
        message = message.encode('utf-8')
        signature = hmac.new(BITSTAMP_API_SECRET.encode('utf-8'), msg=message, digestmod=hashlib.sha256).hexdigest()
        
        return {
            "X-Auth": f"BITSTAMP {BITSTAMP_API_KEY}",
            "X-Auth-Signature": signature,
            "X-Auth-Nonce": nonce,
            "X-Auth-Timestamp": timestamp,
            "X-Auth-Version": "v2"
        }
    
    def on_message(self, ws, message):
        """Handle incoming WebSocket messages."""
        try:
            data = json.loads(message)
            
            # Check for trade events
            if 'event' in data and data['event'] == 'trade' and 'data' in data:
                trade_data = data['data']
                
                # Check if this is a buy trade for XRPUSD
                if (int(trade_data['type']) == 0  # Buy trade
                        and data['channel'] == 'live_trades_xrpusd'):
                    
                    # Format the data with microsecond precision
                    timestamp = int(trade_data['timestamp'])
                    current_datetime = datetime.datetime.now()
                    trade_datetime = datetime.datetime.fromtimestamp(timestamp)
                    
                    # Combine the trade timestamp with current microseconds
                    microsecond_datetime = trade_datetime.replace(microsecond=current_datetime.microsecond)
                    
                    formatted_trade = {
                        'id': trade_data['id'],
                        'datetime': microsecond_datetime,
                        'datetime_str': microsecond_datetime.strftime('%Y-%m-%d %H:%M:%S.%f'),
                        'symbol': 'XRPUSD',
                        'side': 'Buy',
                        'amount': float(trade_data['amount']),
                        'price': float(trade_data['price'])
                    }
                    
                    # Log and add to queue for processing
                    logger.info(f"Execution: ID: {formatted_trade['id']}, "
                              f"DateTime: {formatted_trade['datetime_str']}, "
                              f"Symbol: {formatted_trade['symbol']}, "
                              f"Side: {formatted_trade['side']}, "
                              f"Quantity: {formatted_trade['amount']}, "
                              f"Rate: {formatted_trade['price']}")
                    
                    self.execution_queue.put(formatted_trade)
        
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
        
        # Subscribe only to XRPUSD trades
        subscription = {
            "event": "bts:subscribe",
            "data": {
                "channel": "live_trades_xrpusd"
            }
        }
        ws.send(json.dumps(subscription))
        logger.info("Subscribed to XRPUSD trades")
    
    def keep_alive(self):
        """Send periodic heartbeat messages."""
        while self.running_event.is_set():
            try:
                if self.ws is not None:
                    self.ws.send(json.dumps({"event": "bts:heartbeat"}))
                time.sleep(30)
            except Exception as e:
                logger.error(f"Bitstamp heartbeat error: {e}")
                break
    
    def start(self):
        """Start the WebSocket connection."""
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
        
        # Run WebSocket in a separate thread
        self.ws_thread = threading.Thread(target=self.ws.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()
        
        return self.ws, self.ws_thread
    
    def stop(self):
        """Stop the WebSocket connection."""
        if self.ws is not None:
            self.ws.close()
