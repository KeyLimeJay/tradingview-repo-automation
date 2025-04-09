#position_websocket.py
#!/usr/bin/env python3
import asyncio
import websockets
import json
import requests
import logging
from threading import Thread, Event
from datetime import datetime
import time
import os
import math
from dotenv import load_dotenv

load_dotenv()

def truncate_balance(symbol, balance):
    """
    Truncate balance to appropriate precision based on currency
    
    Args:
        symbol: Trading pair symbol (e.g., "BTC/USDC" or currency name "BTC")
        balance: The balance to truncate
        
    Returns:
        Truncated balance
    """
    # Extract currency if full trading pair is provided
    currency = symbol.split('/')[0] if '/' in symbol else symbol
    
    # Define truncation decimals by currency
    truncation_decimals = {
        'BTC': 3,  # Keep 3 decimal places (0.001)
        'ETH': 2,  # Keep 2 decimal places (0.01)
    }
    
    # Default decimals for other currencies
    default_decimals = 2
    
    # Get decimal places for this currency
    decimals = truncation_decimals.get(currency, default_decimals)
    
    # Handle zero or very small values to avoid rounding issues
    if abs(balance) < 1e-10:
        return 0.0
    
    # Ensure truncation rounds down for positive and up for negative values (towards zero)
    factor = 10 ** decimals
    truncated = math.floor(abs(balance) * factor) / factor * (1 if balance >= 0 else -1)
    
    # Safety check to ensure truncation doesn't increase position
    if abs(truncated) > abs(balance):
        return balance  # Use raw value as fallback
        
    return truncated

class PositionWebsocketClient:
    def __init__(self, api_key=None, api_secret=None, logger=None, auto_reconnect=True, reconnect_interval=5):
        self.base_url = os.getenv("API_BASE_URL")
        self.ws_url = os.getenv("WS_URL")
        self.session = requests.Session()
        self.auth_token = None
        self.logger = logger or logging.getLogger(__name__)
        self.positions = {}
        self.repos = {}
        self._ws_thread = None
        self._stop_event = Event()
        self._connected = Event()
        self.auto_reconnect = auto_reconnect
        self.reconnect_interval = reconnect_interval
        self._last_message_time = time.time()
        self._heartbeat_interval = 30
        self.api_key = api_key or os.getenv("API_KEY")
        self.api_secret = api_secret or os.getenv("API_SECRET")
        self._last_refresh_time = 0
        self._min_refresh_interval = 1  # Minimum seconds between refreshes
        
    async def login(self):
        """Authenticate with the API to get a token for WebSocket connection"""
        try:
            self.session.headers.update({
                'Content-Type': 'application/json',
                'Accept': '*/*',
                'Origin': self.base_url,
                'Referer': f'{self.base_url}/login?noredir=1'
            })
            
            login_data = {
                "username": os.getenv("API_USERNAME"),
                "password": os.getenv("API_PASSWORD"),
                "code": os.getenv("API_CODE"),
                "redirectTo": f"{self.base_url}/trader"
            }
            
            response = self.session.post(
                f"{self.base_url}/sso/api/login",
                json=login_data
            )
            
            if response.status_code == 200:
                token = response.headers.get('Authorization', response.headers.get('authorization'))
                if token and token.startswith('Bearer '):
                    # Store without Bearer prefix for websocket
                    self.auth_token = token.replace('Bearer ', '')
                else:
                    self.auth_token = token
                    
                self.logger.info("Authentication successful")
                return True
            else:
                self.logger.error(f"Authentication failed: {response.status_code} - {response.text}")
                return False
                    
        except Exception as e:
            self.logger.error(f"Authentication error: {str(e)}")
            return False

    async def connect_websocket(self):
        """Connect to WebSocket and handle messages"""
        if not self.auth_token:
            self.logger.error("No auth token available for WebSocket connection")
            return
            
        token = self.auth_token
        
        while not self._stop_event.is_set():
            try:
                self.logger.info(f"Connecting to WebSocket: {self.ws_url}")
                async with websockets.connect(
                    self.ws_url,
                    extra_headers={
                        'Origin': self.base_url,
                        'User-Agent': 'Mozilla/5.0',
                        'Cookie': 'mfaEnabledUser=false; mfaEnabledTm=false'
                    },
                    subprotocols=[token]
                ) as ws:
                    self.logger.info("WebSocket connected")
                    self._connected.set()
                    
                    await self.send_subscriptions(ws)
                    
                    # Use a simpler approach for heartbeat, using a task in the main loop
                    # instead of a separate background task
                    last_heartbeat = time.time()
                    
                    while not self._stop_event.is_set():
                        try:
                            # Set a reasonable timeout for recv
                            try:
                                # Use wait_for with a timeout
                                message = await asyncio.wait_for(ws.recv(), timeout=5)
                                self._last_message_time = time.time()
                                await self.handle_message(message)
                            except asyncio.TimeoutError:
                                # Check if we need to send a heartbeat
                                current_time = time.time()
                                if current_time - last_heartbeat > self._heartbeat_interval:
                                    await ws.ping()
                                    self.logger.debug("Sent heartbeat ping")
                                    last_heartbeat = current_time
                        except websockets.exceptions.ConnectionClosed:
                            self.logger.warning("WebSocket connection closed")
                            break
                        
            except Exception as e:
                self.logger.error(f"WebSocket connection error: {str(e)}")
                self._connected.clear()
                
                if self.auto_reconnect and not self._stop_event.is_set():
                    self.logger.info(f"Attempting reconnection in {self.reconnect_interval} seconds...")
                    await asyncio.sleep(self.reconnect_interval)
                else:
                    break

    async def send_subscriptions(self, ws):
        """Send subscription messages to WebSocket"""
        subscriptions = [
            {"type": "lmorder.subscribe"},
            {"type": "balance.subscribe"},
            {"type": "order.subscribe"}
        ]
        for sub in subscriptions:
            await ws.send(json.dumps(sub))
            self.logger.debug(f"Sent subscription: {sub}")

    async def handle_message(self, message):
        """Process messages received from WebSocket"""
        try:
            if not message or message.isspace():
                return

            data = json.loads(message)
            message_type = data.get('type', '')
            
            if message_type == 'balance':
                await self.handle_balance_update(data)
            elif message_type in ['lmorder', 'order']:
                await self.handle_order_update(data)
            else:
                self.logger.debug(f"Received message of type: {message_type}")
                
        except json.JSONDecodeError:
            self.logger.warning(f"Received invalid JSON message: {message[:100]}...")
        except Exception as e:
            self.logger.error(f"Error handling message: {str(e)}")

    async def handle_balance_update(self, data):
        """Handle balance update messages"""
        try:
            await self._process_balance_update(data)
        except Exception as e:
            self.logger.error(f"Error processing balance update: {str(e)}")

    async def _process_balance_update(self, data):
        """Process balance data from API or WebSocket"""
        content = data.get('content', [])
        positions_update = {}
        
        for balance in content:
            symbol = balance.get('symbol')
            if symbol:
                # Skip non-symbol balances
                if not '/' in symbol:
                    continue
                    
                # Check if this is a repo symbol
                is_repo = 'USDC110' in symbol
                base_symbol = symbol
                
                # If it's a repo symbol, track the base symbol for repo status
                if is_repo:
                    parts = symbol.split('/')
                    if len(parts) > 0:
                        base_currency = parts[0]
                        base_symbol = f"{base_currency}/USDC"
                        self.repos[base_symbol] = True
                        self.logger.debug(f"Tracking repo for base symbol: {base_symbol}")
                
                # Store raw values in our positions tracking
                raw_quantity = float(balance.get('available', 0))
                
                positions_update[symbol] = {
                    'quantity': raw_quantity,
                    'available': raw_quantity,
                    'pending': float(balance.get('pending', 0)),
                    'has_repo': self.repos.get(base_symbol, False),
                    'last_update': datetime.now().isoformat()
                }
        
        if positions_update:
            self.positions.update(positions_update)
            self.logger.debug(f"Updated positions for {len(positions_update)} symbols")

    async def handle_order_update(self, data):
        """Handle order update messages"""
        try:
            content = data.get('content', {})
            symbol = content.get('symbol', '')
            
            # Process repo status updates
            if 'USDC110' in symbol:
                status = content.get('ordStatus')
                base_symbol = None
                
                # Extract base symbol (e.g. BTC/USDC from BTC/USDC110)
                if symbol:
                    parts = symbol.split('/')
                    if len(parts) > 0:
                        base_currency = parts[0]
                        base_symbol = f"{base_currency}/USDC"
                
                if base_symbol:
                    if status == 'FILLED':
                        self.repos[base_symbol] = True
                        self.logger.info(f"Repo activated for {base_symbol}")
                    elif status in ['CANCELLED', 'REJECTED', 'EXPIRED']:
                        self.repos[base_symbol] = False
                        self.logger.info(f"Repo deactivated for {base_symbol}")
                
                # Optionally notify webhook of repo updates
                try:
                    port = os.getenv('PORT', 6101)
                    webhook_url = f"http://localhost:{port}/webhook/repo"
                    requests.post(webhook_url, json=content, timeout=2)
                except Exception as e:
                    # Don't log webhook errors as critical - this is optional
                    self.logger.debug(f"Failed to send repo webhook: {e}")
                    
        except Exception as e:
            self.logger.error(f"Error processing order update: {e}")

    def refresh_positions(self):
        """Force a position refresh from the API"""
        # Rate limit refreshes to avoid hammering the API
        current_time = time.time()
        if current_time - self._last_refresh_time < self._min_refresh_interval:
            self.logger.debug("Skipping refresh due to rate limiting")
            return False
            
        self._last_refresh_time = current_time
        
        try:
            # Make direct API call to get current positions
            from trading_utils import get_jwt_token
            
            jwt_token = get_jwt_token()
            if not jwt_token:
                self.logger.error("Failed to get JWT token for position refresh")
                return False
                
            # Call balances API
            headers = {"Authorization": jwt_token}
            response = requests.get(f"{self.base_url}/rest/balances", headers=headers)
            
            if response.ok:
                data = response.json()
                
                # Process the balance data
                async def process_data():
                    await self._process_balance_update(data)
                
                # Run the async function in the background
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(process_data())
                finally:
                    loop.close()
                
                self.logger.info("Positions refreshed from API")
                return True
            else:
                self.logger.error(f"Failed to refresh positions: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error refreshing positions: {str(e)}")
            return False

    def set_repo_status(self, symbol, has_repo):
        """Manually set repo status for a symbol"""
        base_currency = symbol.split('/')[0] if '/' in symbol else symbol
        base_symbol = f"{base_currency}/USDC"
        self.repos[base_symbol] = has_repo
        self.logger.info(f"Manually set repo status for {base_symbol}: {has_repo}")
        return True

    def is_connected(self):
        """Check if WebSocket is connected"""
        return self._connected.is_set()

    def get_position(self, symbol):
        """Get the current position for a symbol"""
        position_data = self.positions.get(symbol, {})
        quantity = position_data.get('quantity', 0)
        return quantity
    
    def get_truncated_position(self, symbol):
        """Get the current position for a symbol with truncation applied"""
        raw_position = self.get_position(symbol)
        truncated = truncate_balance(symbol, raw_position)
        
        # Safety check to ensure truncation doesn't increase position
        if abs(truncated) > abs(raw_position):
            self.logger.warning(f"Truncation error: truncated value {truncated} exceeds raw value {raw_position}")
            return raw_position  # Use raw value as fallback
            
        return truncated

    def has_repo(self, symbol):
        """Check if we have an active repo for this symbol"""
        if '/' in symbol:
            base_currency = symbol.split('/')[0]
            base_symbol = f"{base_currency}/USDC"
            return self.repos.get(base_symbol, False)
        return self.repos.get(symbol, False)

    def get_all_positions(self):
        """Get all current positions"""
        return self.positions
        
    def get_all_truncated_positions(self):
        """Get all current positions with truncation applied"""
        truncated_positions = {}
        for symbol, position_data in self.positions.items():
            position_copy = position_data.copy()
            raw_quantity = position_data.get('quantity', 0)
            position_copy['quantity'] = truncate_balance(symbol, raw_quantity)
            position_copy['truncated'] = True
            truncated_positions[symbol] = position_copy
        return truncated_positions

    async def _run(self):
        """Main loop for WebSocket client"""
        while not self._stop_event.is_set():
            try:
                if await self.login():
                    await self.connect_websocket()
                else:
                    self.logger.error("Failed to login")
                    if self.auto_reconnect:
                        await asyncio.sleep(self.reconnect_interval)
                    else:
                        break
            except Exception as e:
                self.logger.error(f"Run loop error: {str(e)}")
                if self.auto_reconnect:
                    await asyncio.sleep(self.reconnect_interval)
                else:
                    break

    def start(self):
        """Start the WebSocket client in a background thread"""
        if self._ws_thread and self._ws_thread.is_alive():
            self.logger.warning("WebSocket client already running")
            return

        # Reset repo cache on startup
        self.repos = {}
        self.logger.info("Repo status cache cleared on startup")

        def run_loop_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._run())
            except Exception as e:
                self.logger.error(f"Error in WebSocket thread: {e}")
            finally:
                loop.close()

        self._stop_event.clear()
        self._ws_thread = Thread(target=run_loop_in_thread, daemon=True)
        self._ws_thread.start()
        self.logger.info("WebSocket client started in background thread")
        
        # Perform initial position refresh
        time.sleep(2)  # Give WebSocket time to connect
        self.refresh_positions()

    def stop(self):
        """Stop the WebSocket client"""
        try:
            self._stop_event.set()
            self._connected.clear()
            
            if self._ws_thread:
                self._ws_thread.join(timeout=5)
                if self._ws_thread.is_alive():
                    self.logger.warning("WebSocket thread did not terminate cleanly")
                
            self.logger.info("WebSocket client stopped")
            
        except Exception as e:
            self.logger.error(f"Error stopping WebSocket client: {e}")
            raise

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()