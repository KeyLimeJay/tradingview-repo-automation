#!/usr/bin/env python3
import asyncio
import websockets
import json
import csv
import os
import logging
import time
import datetime
from pathlib import Path
from collections import defaultdict

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('xrp_price_monitor')

class XRPPriceMonitor:
    def __init__(self, symbol="XRP/USDT"):
        # Hard-coded credentials
        self.api_key = "a9827239-53af-46da-9f67-84b2523b10e5"
        self.api_secret = "pids1hCi5KmSTA9sie2hY1jcXGc7Tlggs6x1"
        self.api_username = "tv-algo-1@bosonic.digital"
        self.api_password = "Prod1234$"
        self.api_code = "NWY3YTFjODExODUwODQ3N2I3MzUzYjcz"
        self.api_url = "https://api.bosonic.digital"
        self.api_base_url = "https://trad6.bosonic.digital"
        self.ws_url = "wss://trad6.bosonic.digital/websocket/trader"
        self.custodian_id = "SAFETRUST"
        
        # Symbol and CSV setup
        self.symbol = symbol
        self.csv_filename = f"{symbol.replace('/', '_')}_minute_aggregated.csv"
        self._initialize_csv()
        
        # Tracking variables
        self._stop_event = asyncio.Event()
        self.auth_token = None
        
        # Minute aggregation data
        self.current_minute_data = defaultdict(list)
        self.last_processed_minute = None
        
        # Schedule minute processing
        self.minute_processor_task = None
        
        # Flag to log first few messages for debugging
        self.debug_count = 0
        
    def _initialize_csv(self):
        """Initialize CSV file with headers if it doesn't exist"""
        # Modified for minute aggregation and VWAP
        headers = [
            'timestamp', 
            'symbol', 
            'total_bid_size',
            'vwap'
        ]
        
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
            logger.info(f"Created new CSV file: {self.csv_filename}")
        else:
            logger.info(f"Using existing CSV file: {self.csv_filename}")
    
    def _append_to_csv(self, data_dict):
        """Append aggregated market data to CSV file"""
        try:
            # Extract fields for the aggregated data
            row = [
                data_dict.get('timestamp'),
                data_dict.get('symbol'),
                data_dict.get('total_bid_size'),
                data_dict.get('vwap')
            ]
            
            with open(self.csv_filename, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(row)
            logger.info(f"Saved minute data: {row}")
        except Exception as e:
            logger.error(f"Error saving to CSV: {str(e)}")
    
    async def process_minute_data(self):
        """Process and save data for each completed minute"""
        try:
            while not self._stop_event.is_set():
                current_time = datetime.datetime.now()
                current_minute = current_time.replace(second=0, microsecond=0)
                
                # Process previous minute if we've moved to a new minute
                if self.last_processed_minute and current_minute > self.last_processed_minute:
                    minute_to_process = self.last_processed_minute
                    minute_key = minute_to_process.strftime("%Y-%m-%d %H:%M")
                    
                    if minute_key in self.current_minute_data and self.current_minute_data[minute_key]:
                        # Calculate VWAP
                        price_size_products = 0
                        total_size = 0
                        
                        for price, size in self.current_minute_data[minute_key]:
                            price_size_products += price * size
                            total_size += size
                        
                        if total_size > 0:
                            vwap = price_size_products / total_size
                            
                            # Save the aggregated data
                            aggregated_data = {
                                'timestamp': minute_to_process.isoformat(),
                                'symbol': self.symbol,
                                'total_bid_size': total_size,
                                'vwap': vwap
                            }
                            
                            self._append_to_csv(aggregated_data)
                        
                        # Remove processed data
                        del self.current_minute_data[minute_key]
                
                # Update the last processed minute
                self.last_processed_minute = current_minute
                
                # Wait for next processing cycle (check every 5 seconds)
                await asyncio.sleep(5)
                
        except Exception as e:
            logger.error(f"Error processing minute data: {str(e)}")
    
    async def login(self):
        """Authenticate with the API to get a token for WebSocket connection"""
        try:
            import requests
            
            session = requests.Session()
            session.headers.update({
                'Content-Type': 'application/json',
                'Accept': '*/*',
                'Origin': self.api_base_url,
                'Referer': f'{self.api_base_url}/login?noredir=1'
            })
            
            login_data = {
                "username": self.api_username,
                "password": self.api_password,
                "code": self.api_code,
                "redirectTo": f"{self.api_base_url}/trader"
            }
            
            response = session.post(
                f"{self.api_base_url}/sso/api/login",
                json=login_data
            )
            
            if response.status_code == 200:
                token = response.headers.get('Authorization', response.headers.get('authorization'))
                if token and token.startswith('Bearer '):
                    # Store without Bearer prefix for websocket
                    self.auth_token = token.replace('Bearer ', '')
                else:
                    self.auth_token = token
                    
                logger.info("Authentication successful")
                return True
            else:
                logger.error(f"Authentication failed: {response.status_code} - {response.text}")
                return False
                    
        except Exception as e:
            logger.error(f"Authentication error: {str(e)}")
            return False
    
    async def connect_websocket(self):
        """Connect to WebSocket and handle messages"""
        if not self.auth_token:
            success = await self.login()
            if not success:
                logger.error("No auth token available for WebSocket connection")
                return
            
        token = self.auth_token
        
        while not self._stop_event.is_set():
            try:
                logger.info(f"Connecting to WebSocket: {self.ws_url}")
                async with websockets.connect(
                    self.ws_url,
                    extra_headers={
                        'Origin': self.api_base_url,
                        'User-Agent': 'Mozilla/5.0',
                        'Cookie': 'mfaEnabledUser=false; mfaEnabledTm=false'
                    },
                    subprotocols=[token]
                ) as ws:
                    logger.info("WebSocket connected")
                    
                    await self.send_subscriptions(ws)
                    
                    # Process messages until stopped
                    last_heartbeat = time.time()
                    
                    while not self._stop_event.is_set():
                        try:
                            # Set a reasonable timeout for recv
                            try:
                                # Use wait_for with a timeout
                                message = await asyncio.wait_for(ws.recv(), timeout=5)
                                await self.handle_message(message)
                            except asyncio.TimeoutError:
                                # Check if we need to send a heartbeat
                                current_time = time.time()
                                if current_time - last_heartbeat > 30:  # 30 seconds heartbeat interval
                                    await ws.ping()
                                    logger.debug("Sent heartbeat ping")
                                    last_heartbeat = current_time
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning("WebSocket connection closed")
                            break
                        
            except Exception as e:
                logger.error(f"WebSocket connection error: {str(e)}")
                
                if not self._stop_event.is_set():
                    logger.info(f"Attempting reconnection in 5 seconds...")
                    await asyncio.sleep(5)
                else:
                    break
    
    async def send_subscriptions(self, ws):
        """Send subscription messages to WebSocket"""
        subscriptions = [
            {"type": "ticker.subscribe"},
            {"type": "marketdata.subscribe"}
        ]
        for sub in subscriptions:
            await ws.send(json.dumps(sub))
            logger.debug(f"Sent subscription: {sub}")
    
    async def handle_message(self, message):
        """Process messages received from WebSocket"""
        try:
            if not message or message.isspace():
                return

            data = json.loads(message)
            message_type = data.get('type', '')
            
            # Log a few initial messages to understand structure (for debugging)
            if self.debug_count < 5:
                logger.info(f"Message sample: {message[:300]}...")
                self.debug_count += 1
            
            if message_type == 'ticker':
                await self.handle_ticker_update(data)
            elif message_type == 'marketdata':
                await self.handle_marketdata_update(data)
                
        except json.JSONDecodeError:
            logger.warning(f"Received invalid JSON message: {message[:100]}...")
        except Exception as e:
            logger.error(f"Error handling message: {str(e)}")
    
    async def handle_ticker_update(self, data):
        """Handle ticker update messages (for price data)"""
        try:
            content = data.get('content', {})
            symbol = content.get('symbol')
            
            # Skip if not our target symbol
            if symbol != self.symbol:
                return
                
            # Extract bid price and size
            bid_price = None
            bid_size = None
            
            for field in ['bid', 'bidPx', 'bidPrice']:
                if field in content:
                    bid_price = float(content.get(field, 0))
                    break
                    
            for field in ['bidSize', 'bidQty', 'bidQuantity']:
                if field in content:
                    bid_size = float(content.get(field, 0))
                    break
            
            # Only process if we have both bid price and size
            if bid_price is not None and bid_size is not None and bid_price > 0 and bid_size > 0:
                # Add to current minute data
                current_time = datetime.datetime.now()
                minute_key = current_time.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")
                
                self.current_minute_data[minute_key].append((bid_price, bid_size))
                logger.debug(f"Added to minute {minute_key}: price={bid_price}, size={bid_size}")
                
        except Exception as e:
            logger.error(f"Error processing ticker update: {str(e)}")
    
    async def handle_marketdata_update(self, data):
        """Handle market data messages with detailed bid information"""
        try:
            content = data.get('content', {})
            symbol = content.get('symbol')
            
            # Skip if not our target symbol
            if symbol != self.symbol:
                return
                
            # Extract bids
            bids = content.get('bids', [])
            
            if not bids:
                return
                
            # Process best bid
            bid = bids[0]  # Best bid is first in list
            
            # Extract quote price (bid price)
            bid_price = None
            bid_size = None
            
            for field in ['quotePx', 'price', 'px']:
                if field in bid:
                    bid_price = float(bid.get(field, 0))
                    break
            
            # Extract quote size (bid quantity)
            for field in ['quoteSize', 'size', 'qty', 'quantity']:
                if field in bid:
                    bid_size = float(bid.get(field, 0))
                    break
            
            # Only process if we have both bid price and size
            if bid_price is not None and bid_size is not None and bid_price > 0 and bid_size > 0:
                # Add to current minute data
                current_time = datetime.datetime.now()
                minute_key = current_time.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")
                
                self.current_minute_data[minute_key].append((bid_price, bid_size))
                logger.debug(f"Added to minute {minute_key}: price={bid_price}, size={bid_size}")
                    
        except Exception as e:
            logger.error(f"Error processing market data update: {str(e)}")
    
    async def run(self):
        """Run the price monitor"""
        try:
            self._stop_event.clear()
            
            # Start the minute data processor using the older method
            self.minute_processor_task = asyncio.ensure_future(self.process_minute_data())
            
            # Set initial last processed minute
            self.last_processed_minute = datetime.datetime.now().replace(second=0, microsecond=0)
            
            # Connect to websocket
            await self.connect_websocket()
        except Exception as e:
            logger.error(f"Error in price monitor: {str(e)}")
        finally:
            # Cancel the minute processor task if it's running
            if self.minute_processor_task and not self.minute_processor_task.done():
                self.minute_processor_task.cancel()
                try:
                    await self.minute_processor_task
                except asyncio.CancelledError:
                    pass
        
    def stop(self):
        """Stop the price monitor"""
        self._stop_event.set()
        logger.info("Price monitor stopped")

async def main():
    # Create and run price monitor
    monitor = XRPPriceMonitor(symbol="XRP/USDT")
    
    try:
        await monitor.run()
    except KeyboardInterrupt:
        monitor.stop()
        logger.info("Program terminated by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
    finally:
        monitor.stop()

if __name__ == "__main__":
    # Set up the event loop for older Python versions
    try:
        import asyncio
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Program terminated by user")
    finally:
        # Check if the loop is not already closed
        if not loop.is_closed():
            loop.close()
