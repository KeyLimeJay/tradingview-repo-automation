# bosonic_client.py
"""
Bosonic WebSocket client for receiving bid data.
"""
import json
import time
import datetime
import logging
import asyncio
import requests
import websockets
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
        self.loop = None
        self.task = None
        self.ws = None
    
    async def login(self):
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
    
    async def send_subscriptions(self, ws):
        """Send subscription messages to WebSocket."""
        subscriptions = [
            {"type": "ticker.subscribe"},
            {"type": "marketdata.subscribe"}
        ]
        for sub in subscriptions:
            await ws.send(json.dumps(sub))
            logger.debug(f"Sent Bosonic subscription: {sub}")
    
    async def handle_message(self, message):
        """Process messages received from WebSocket."""
        try:
            if not message or message.isspace():
                return

            data = json.loads(message)
            message_type = data.get('type', '')
            
            if message_type == 'ticker':
                await self.handle_ticker_update(data)
            elif message_type == 'marketdata':
                await self.handle_marketdata_update(data)
                
        except json.JSONDecodeError:
            logger.warning(f"Received invalid JSON message from Bosonic: {message[:100]}...")
        except Exception as e:
            logger.error(f"Error handling Bosonic message: {str(e)}")
    
    async def handle_ticker_update(self, data):
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
    
    async def handle_marketdata_update(self, data):
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
    
    async def run_websocket(self):
        """Run the WebSocket connection."""
        while self.running_event.is_set():
            try:
                token = await self.login()
                if not token:
                    logger.error("Failed to obtain Bosonic authentication token, cannot connect to WebSocket")
                    await asyncio.sleep(30)  # Wait before trying again
                    continue
                
                logger.info(f"Connecting to Bosonic WebSocket: {BOSONIC_WS_URL}")
                
                # Establish WebSocket connection - using the older style approach
                try:
                    # Create a websocket connection
                    headers = {
                        'Origin': BOSONIC_API_BASE_URL,
                        'User-Agent': 'Mozilla/5.0',
                        'Authorization': f'Bearer {token}'
                    }
                    
                    # Create a websocket connection manually without using async with
                    ws = await websockets.connect(
                        BOSONIC_WS_URL,
                        extra_headers=headers,
                        subprotocols=[token] if token else None
                    )
                    
                    logger.info("Bosonic WebSocket connected")
                    
                    # Send subscriptions
                    await self.send_subscriptions(ws)
                    
                    # Process messages
                    while self.running_event.is_set():
                        try:
                            # Wait for message with timeout
                            message = await asyncio.wait_for(ws.recv(), timeout=30)
                            await self.handle_message(message)
                        except asyncio.TimeoutError:
                            # Send periodic ping to keep connection alive
                            try:
                                pong_waiter = await ws.ping()
                                await asyncio.wait_for(pong_waiter, timeout=10)
                                logger.debug("Sent Bosonic WebSocket ping")
                            except Exception as ping_error:
                                logger.warning(f"Failed to send ping: {str(ping_error)}")
                                break
                        except websockets.exceptions.ConnectionClosed as cc:
                            logger.warning(f"Bosonic WebSocket connection closed: {str(cc)}")
                            break
                        
                    # Make sure to close the connection when done
                    await ws.close()
                    
                except Exception as conn_error:
                    logger.error(f"WebSocket connection error: {conn_error}")
            
            except Exception as e:
                logger.error(f"Unexpected Bosonic WebSocket error: {e}")
                
            # Wait before attempting to reconnect
            if self.running_event.is_set():
                logger.info("Attempting to reconnect to Bosonic in 5 seconds...")
                await asyncio.sleep(5)
    
    def start(self):
        """Start the client in a new event loop."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        
        # Create and start the task
        self.task = self.loop.create_task(self.run_websocket())
        
        # Run the loop in a thread
        def run_asyncio_loop():
            try:
                self.loop.run_forever()
            except Exception as e:
                logger.error(f"Error in event loop: {e}")
            finally:
                self.loop.close()
                
        import threading
        asyncio_thread = threading.Thread(target=run_asyncio_loop)
        asyncio_thread.daemon = True
        asyncio_thread.start()
        
        return asyncio_thread
    
    def stop(self):
        """Stop the client."""
        if self.loop and not self.loop.is_closed():
            # Schedule stopping the loop
            self.loop.call_soon_threadsafe(self.loop.stop)