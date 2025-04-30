#!/usr/bin/env python3
import os
import json
import time
import hmac
import hashlib
import base64
import requests
import logging
import random
import string
import datetime
from flask import Flask, request, jsonify
from logging.handlers import RotatingFileHandler
from threading import Lock
from collections import OrderedDict

# Import necessary modules from src
from src.position_websocket import PositionWebsocketClient
from src.trading_utils import place_order, OrderPlacementError
from src.account_manager import AccountManager
from src.auto_position_manager import AutoPositionManager
from src.config_manager import ConfigurationManager

class TradingBot:
    """
    Main trading bot implementation with logic based on event sequence table.
    Refactored to ensure proper repo handling.
    """
    
    def __init__(self, config_path=None):
        """Initialize the trading bot with configuration."""
        self.app = Flask(__name__)
        self.webhook_lock = Lock()
        
        # Load configuration
        self.config_manager = ConfigurationManager(config_path)
        
        # Set up logging
        self.logger = self.setup_logging()
        
        # Initialize account manager
        self.account_manager = AccountManager(self.config_manager, logger=self.logger)
        self.logger.info("Loading account configurations...")
        num_accounts = self.account_manager.initialize_accounts()
        self.logger.info(f"Loaded {num_accounts} account configurations")
        
        # Initialize position clients for all accounts
        self.account_manager.initialize_position_clients()
        
        # Initialize auto position manager
        self.auto_position_manager = AutoPositionManager(
            self.config_manager, self.account_manager)
        
        self._request_cache = OrderedDict()
        self.trading_pairs = self.config_manager.get_all_trading_pairs()
        
        # Event tracking
        self.event_counter = {}  # account_name:symbol -> event_count
        self.sequence_history = {}  # account_name:symbol -> [event1, event2, ...]
        self.sequence_type = {}  # account_name:symbol -> "Buy Start" or "Sell Start"
        
        # Separate last_signal tracking per account
        self.last_signals = {}  # account_name -> symbol -> {message, timeframe, timestamp}
        
        # Signal rate limiting across all accounts
        self._signal_timestamps = {}  # "symbol:message:timeframe" -> timestamp
        
        # Set up Flask routes
        self.setup_routes()
    
    def setup_logging(self):
        """Set up logging configuration."""
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        # Get logging configuration from config
        log_level = self.config_manager.get_global_setting('log_level', 'INFO')
        max_bytes = int(self.config_manager.get_global_setting('log_max_bytes', 10000000))
        backup_count = int(self.config_manager.get_global_setting('log_backup_count', 5))
        
        file_handler = RotatingFileHandler(
            os.path.join(log_dir, 'trading_bot.log'),
            maxBytes=max_bytes,
            backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        logger = logging.getLogger('system')
        logger.setLevel(log_level)
        logger.handlers = []
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def setup_routes(self):
        """Set up Flask routes."""
        self.app.add_url_rule('/webhook', view_func=self.webhook, methods=['POST'])
        self.app.add_url_rule('/positions', view_func=self.get_positions, methods=['GET'])
        self.app.add_url_rule('/health', view_func=self.health_check, methods=['GET'])
        self.app.add_url_rule('/auto-short', view_func=self.trigger_auto_short, methods=['POST'])
        self.app.add_url_rule('/sequence-reset', view_func=self.handle_sequence_reset, methods=['POST'])
        self.app.add_url_rule('/sequence-health', view_func=self.sequence_health, methods=['GET'])
        # Add route for direct repo testing
        self.app.add_url_rule('/test-repo', view_func=self.test_repo, methods=['POST'])
    
    def generate_repo_clordid(self):
        """Generate a unique client order ID for repo orders"""
        now = time.strftime("%Y%m%d%H%M%S")
        random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))
        return f"WEB:{random_suffix}-{now}"
    
    def place_repo_order_direct(self, credentials, symbol, quantity, interest_rate):
        """
        Place a repo order using direct API call with provided credentials
        
        Args:
            credentials: Dictionary containing API credentials
            symbol: Repo symbol (e.g., "ETH/USDC110")
            quantity: Amount to borrow
            interest_rate: Interest rate (e.g., 10.0)
            
        Returns:
            API response or None if failed
        """
        self.logger.info(f"Placing repo order for {symbol}, quantity={quantity}, interest_rate={interest_rate}%")
        
        # Set up URL
        url = f"{credentials.get('api_url')}/rest/orders"
        endpoint = "/rest/orders"
        
        # Generate a unique repo client order ID
        clordid = self.generate_repo_clordid()
        
        # Create repo order data
        order_data = {
            "side": "BID",  # BID for borrowing
            "price": float(interest_rate),
            "custodianId": credentials.get('custodian_id'),
            "symbol": symbol,
            "currency": symbol.split('/')[0],
            "currency2": "USDC110",
            "orderQty": float(quantity),
            "clOrdId": clordid,
            "orderType": "LIMIT",
            "tif": "GTC",
            "dark": False,
            "isAvgPrice": False,
            "venue": "LIT"
        }
        
        # Convert order data to JSON
        body = json.dumps(order_data)
        
        # Create signature using body hash method
        body_hash = base64.b64encode(hashlib.md5(body.encode()).digest()).decode()
        string_to_sign = f"POST\n{endpoint}\n{body_hash}"
        signature = base64.b64encode(
            hmac.new(credentials.get('api_secret').encode(), string_to_sign.encode(), hashlib.sha256).digest()
        ).decode()
        
        # Set up headers
        headers = {
            'api-key': credentials.get('api_key'),
            'api-sign': signature,
            'Content-Type': 'application/json'
        }
        
        self.logger.info(f"Making API request to {url}")
        self.logger.debug(f"Headers: {headers}")
        self.logger.debug(f"Body: {body}")
        
        try:
            # Make the API request
            response = requests.post(url, headers=headers, data=body, timeout=30)
            
            if response.ok:
                self.logger.info(f"Repo order placed successfully: {response.status_code}")
                if response.text:
                    return response.json()
                return {"status": "success"}
            else:
                self.logger.error(f"Error placing repo order: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            self.logger.error(f"Exception placing repo order: {str(e)}")
            return None
    
    def get_jwt_token(self, account_name):
        """Get JWT token for API authentication"""
        self.logger.info(f"Getting JWT token for account {account_name}...")
        
        credentials = self.config_manager.get_account_credentials(account_name)
        if not credentials:
            self.logger.error(f"No credentials found for account {account_name}")
            return None
        
        base_url = credentials.get('api_base_url')
        if not base_url.endswith('/'):
            base_url += '/'
        
        url = f"{base_url}sso/api/login"
        
        payload = {
            "username": credentials.get('api_username'),
            "password": credentials.get('api_password'),
            "code": credentials.get('api_code'),
            "redirectTo": f"{base_url}/trader",
            "email": credentials.get('api_username')
        }
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Origin': base_url,
            'Referer': f'{base_url}/login?noredir=1'
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                token = response.headers.get('Authorization', response.headers.get('authorization'))
                if token and token.startswith('Bearer '):
                    token = token.replace('Bearer ', '')
                
                self.logger.info(f"Authentication successful for account {account_name}")
                return token
            else:
                self.logger.error(f"Authentication failed for account {account_name}: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            self.logger.error(f"Error during authentication: {str(e)}")
            return None
    
    def check_existing_repo(self, account_name, symbol):
        """Check if a repo already exists"""
        self.logger.info(f"Checking if repo already exists for {symbol} (account: {account_name})")
        
        # Get JWT token
        jwt_token = self.get_jwt_token(account_name)
        if not jwt_token:
            self.logger.error(f"Failed to get JWT token for account {account_name}")
            return None
        
        credentials = self.config_manager.get_account_credentials(account_name)
        if not credentials:
            self.logger.error(f"No credentials found for account {account_name}")
            return None
        
        # Set headers
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json"
        }
        
        # Form URL
        base_url = credentials.get('api_base_url')
        if not base_url.endswith('/'):
            base_url += '/'
        
        url = f"{base_url}rest/repocontract?sortBy=id&sortDirection=DESC&status=OPEN&repoSymbol={symbol}"
        
        # Create payload
        payload = {
            "userId": credentials.get('api_username'),
            "contractType": "BORROW",
            "eventId": f"event{int(time.time())}",
            "repoSymbol": symbol
        }
        
        try:
            # Make the request
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if not response.ok:
                self.logger.error(f"Failed to check repo status: {response.status_code} - {response.text}")
                return None
            
            repo_data = response.json()
            
            if not repo_data.get("content") or len(repo_data["content"]) == 0:
                self.logger.info(f"No open repo found for {symbol}")
                return None
            
            # Get the first open repo
            repo_contract = repo_data["content"][0]
            repo_id = repo_contract.get("id")
            
            if repo_id:
                self.logger.info(f"Found existing repo for {symbol} with ID: {repo_id}")
                return {"id": repo_id}
            else:
                self.logger.warning(f"No repo ID found in response for {symbol}")
                return None
        except Exception as e:
            self.logger.error(f"Error checking repo status: {str(e)}")
            return None
    
    def close_repo_direct(self, account_name, symbol):
        """Close a repo contract using direct API call"""
        self.logger.info(f"Attempting to close repo for {symbol} (account: {account_name})")
        
        # Get JWT token
        jwt_token = self.get_jwt_token(account_name)
        if not jwt_token:
            self.logger.error(f"Failed to get JWT token for account {account_name}")
            return False
        
        credentials = self.config_manager.get_account_credentials(account_name)
        if not credentials:
            self.logger.error(f"No credentials found for account {account_name}")
            return False
        
        # Get repo details first
        repo_details = self.check_existing_repo(account_name, symbol)
        if not repo_details:
            self.logger.warning(f"No open repo found for {symbol} to close")
            return True  # Consider it a success if there's no repo to close
        
        # Get the repo ID and set up the close request
        repo_id = repo_details["id"]
        
        # Create a new event ID for closing
        close_event_id = f"closeEvent{int(time.time())}"
        
        # Set headers
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json",
            "User-Agent": "python-requests/2.28.1"
        }
        
        # Form URL
        base_url = credentials.get('api_base_url')
        if not base_url.endswith('/'):
            base_url += '/'
        
        # Using GET with URL parameters as in the working reference code
        close_url = f"{base_url}rest/repocontract/close?repoContractId={repo_id}&eventId={close_event_id}"
        
        try:
            # Use GET with URL parameters
            close_response = requests.get(
                url=close_url, 
                headers=headers,
                timeout=30
            )
            
            if not close_response.ok:
                self.logger.error(f"Failed to close repo: {close_response.status_code} - {close_response.text}")
                return False
            
            self.logger.info(f"Successfully closed repo for {symbol}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error closing repo: {str(e)}")
            return False
    
    def validate_request_data(self, data):
        """Validate incoming webhook data."""
        if not isinstance(data, dict):
            raise ValueError(f"Invalid data format. Expected dict, got {type(data)}")
            
        required_fields = ['symbol', 'message', 'price']
        missing_fields = [field for field in required_fields if field not in data]
        
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")
            
        # Get all trading pairs from configuration
        trading_pairs = self.config_manager.get_all_trading_pairs()
        if data['symbol'] not in trading_pairs:
            self.logger.warning(f"Received signal for unsupported symbol: {data['symbol']}")
            raise ValueError(f"Invalid symbol: {data['symbol']}. Supported symbols: {', '.join(trading_pairs)}")
            
        # Get valid messages from configuration
        valid_messages = self.config_manager.get_global_setting(
            'valid_messages', ['Trend Buy!', 'Trend Sell!'])
        if data['message'] not in valid_messages:
            self.logger.warning(f"Received invalid message type: {data['message']}")
            raise ValueError(f"Invalid message format. Expected one of {valid_messages}")
        
        # Handle and validate timeframe
        if 'timeFrame' not in data:
            default_timeframe = self.config_manager.get_global_setting('default_timeframe', '1h')
            data['timeFrame'] = default_timeframe
            self.logger.info(f"No timeFrame provided, using default: {data['timeFrame']}")
        else:
            valid_timeframes = self.config_manager.get_global_setting(
                'valid_timeframes', ['1m', '5m', '15m', '30m', '1h', '4h', '1d'])
            if data['timeFrame'] not in valid_timeframes:
                self.logger.warning(f"Received invalid timeFrame: {data['timeFrame']}")
                raise ValueError(f"Invalid timeFrame: {data['timeFrame']}. Expected one of {valid_timeframes}")
                
        # Get account for this timeframe
        account_name = self.config_manager.get_account_for_timeframe(data['timeFrame'])
        if not account_name:
            self.logger.warning(f"No account configured for timeframe: {data['timeFrame']}")
            raise ValueError(f"No account configured for timeframe: {data['timeFrame']}")
            
        # Add account to the data for later use
        data['account'] = account_name
        
        return True
    
    def determine_trade_side(self, message):
        """Determine trade side from message."""
        if message == 'Trend Buy!':
            return 'BID'
        elif message == 'Trend Sell!':
            return 'ASK'
        raise ValueError(f"Cannot determine trade side from message: {message}")
    
    def verify_repo_status(self, symbol, account_name):
        """Verify repo status for a specific symbol and account."""
        base_currency = symbol.split('/')[0]
        repo_symbol = f"{base_currency}/USDC110"
        
        # Using direct repo check
        repo_details = self.check_existing_repo(account_name, repo_symbol)
        has_repo = repo_details is not None
        
        if has_repo:
            self.logger.info(f"Verified repo exists for {symbol} (account: {account_name})")
        else:
            self.logger.info(f"Verified no repo exists for {symbol} (account: {account_name})")
            
        return has_repo
    
    def get_current_price(self, symbol, account_name):
        """Get current market price for a symbol (placeholder)"""
        # In a real implementation, you would fetch this from your price feed
        if 'BTC' in symbol:
            return 93350.0
        elif 'ETH' in symbol:
            return 1750.0
        return 30000.0  # Fallback price
    
    def get_current_state(self, symbol, account_name):
        """Get the current state of balances and repos for the given symbol."""
        position_client = self.account_manager.get_position_client(account_name)
        if not position_client:
            self.logger.error(f"No position client for account: {account_name}")
            return None
            
        # Force a position refresh
        position_client.refresh_positions()
        
        # Get current position
        balance = position_client.get_truncated_position(symbol)
        
        # Check if repo is open
        has_repo = self.verify_repo_status(symbol, account_name)
        
        # Get repo quantity if repo exists
        repo_quantity = 0
        if has_repo:
            base_currency = symbol.split('/')[0]
            repo_symbol = f"{base_currency}/USDC110"
            repo_details = self.check_existing_repo(account_name, repo_symbol)
            
            if repo_details:
                # If we have repo details, use min_quantity as estimate
                # In a full implementation, you would extract the actual quantity
                base_currency = symbol.split('/')[0]
                repo_quantity = self.config_manager.get_currency_setting(
                    account_name, base_currency, 'min_quantity', 0.001)
        
        return {
            'balance': balance,
            'repo': repo_quantity if has_repo else 0
        }
    
    def log_sequence_state(self, symbol, account_name, event_num, signal_type, current_balance, current_repo, next_expected_event=None):
        """Log the current sequence state in a consistent, parseable format"""
        log_message = (
            f"[SEQUENCE][{account_name}][{symbol}] "
            f"EVENT={event_num} | "
            f"SIGNAL={signal_type} | "
            f"BAL={current_balance:.6f} | "
            f"REPO={current_repo:.6f}"
        )
        
        if next_expected_event:
            log_message += f" | NEXT_EXPECTED={next_expected_event}"
            
        self.logger.info(log_message)
    
    def log_sequence_history(self, symbol, account_name):
        """Log a visual representation of the sequence history"""
        account_symbol_key = f"{account_name}:{symbol}"
        if account_symbol_key not in self.sequence_history:
            self.sequence_history[account_symbol_key] = []
        
        # Get current event
        current_event = self.event_counter.get(account_symbol_key, 0)
        
        # Add to history
        self.sequence_history[account_symbol_key].append(current_event)
        
        # Only keep the last 10 events to avoid overly long logs
        if len(self.sequence_history[account_symbol_key]) > 10:
            self.sequence_history[account_symbol_key] = self.sequence_history[account_symbol_key][-10:]
        
        # Generate visual representation
        history_str = ' → '.join([str(e) for e in self.sequence_history[account_symbol_key]])
        
        # Check for expected patterns in last 4 events
        if len(self.sequence_history[account_symbol_key]) >= 4:
            last_four = ''.join(map(str, self.sequence_history[account_symbol_key][-4:]))
            # Check for valid patterns
            valid_patterns = ['1434', '4343', '2343', '3434']
            valid_pattern = any(pattern in last_four for pattern in valid_patterns)
            status = "✓ VALID" if valid_pattern else "✗ BROKEN"
        else:
            # Not enough history to validate
            status = "⋯ INITIALIZING"
        
        self.logger.info(f"[SEQUENCE_HISTORY][{account_name}][{symbol}] {history_str} | STATUS: {status}")
    
    def validate_sequence(self, symbol, signal_type, account_name, expected_signal=None):
        """
        Validate that signal matches expectations.
        Returns True if valid, False if invalid.
        """
        # If no expectation, always valid
        if expected_signal is None:
            return True
            
        # Check if signal type matches expectation
        if signal_type != expected_signal:
            self.logger.warning(
                f"[SEQUENCE_BREAK][{account_name}][{symbol}] "
                f"Expected signal {expected_signal}, but got {signal_type}. "
                f"Initiating sequence recovery."
            )
            # Trigger recovery procedure
            recovery_success = self.recover_sequence(symbol, account_name)
            if recovery_success:
                self.logger.info(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] Recovery successful, sequence reset")
            else:
                self.logger.error(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] Recovery failed")
            return False
            
        return True
    
    def recover_sequence(self, symbol, account_name):
        """
        Reset positions and restart the sequence for a specific symbol.
        
        This will:
        1. Close any open repos
        2. Sell any remaining balance to get back to zero
        3. Reset the event counter
        4. Restart sequence from the appropriate point
        """
        self.logger.warning(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] Initiating sequence recovery")
        
        # Get current state
        current_state = self.get_current_state(symbol, account_name)
        if not current_state:
            self.logger.error(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] Failed to get current state")
            return False
            
        current_balance = current_state['balance']
        current_repo = current_state['repo']
        
        # Get position client for this account
        position_client = self.account_manager.get_position_client(account_name)
        if not position_client:
            self.logger.error(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] No position client available")
            return False
        
        # Get the unit size for this currency
        base_currency = symbol.split('/')[0]
        unit_size = self.config_manager.get_currency_setting(
            account_name, base_currency, 'min_quantity', 0.001)
        
        # Step 1: Close any open repos
        if current_repo > 0:
            repo_symbol = f"{base_currency}/USDC110"
            self.logger.info(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] Closing repo {repo_symbol}")
            
            success = self.close_repo_direct(account_name, repo_symbol)
            
            if not success:
                self.logger.error(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] Failed to close repo")
                return False
                
            # Refresh positions after repo closing
            time.sleep(1)
            position_client.refresh_positions()
            current_state = self.get_current_state(symbol, account_name)
            current_balance = current_state['balance']
            current_repo = current_state['repo']
        
        # Step 2: Sell remaining balance if any
        if current_balance > 0:
            self.logger.info(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] Selling remaining balance {current_balance}")
            
            # Get latest price for the symbol
            price = self.get_current_price(symbol, account_name)
            if not price:
                self.logger.error(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] Failed to get current price")
                return False
            
            # Adjust price down to ensure fill
            price = price * 0.95  # 5% below market price
            
            # Place sell order
            response = place_order(
                symbol=symbol,
                side='ASK',
                price=price,
                quantity=current_balance,  # Sell entire balance
                config_manager=self.config_manager,
                account_name=account_name
            )
            
            if not response:
                self.logger.error(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] Failed to sell remaining balance")
                return False
                
            # Refresh positions after selling
            time.sleep(1)
            position_client.refresh_positions()
        
        # Step 3: Reset the event counter
        account_symbol_key = f"{account_name}:{symbol}"
        self.logger.info(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] Resetting event counter")
        if account_symbol_key in self.event_counter:
            self.event_counter[account_symbol_key] = 0
        if account_symbol_key in self.sequence_history:
            self.sequence_history[account_symbol_key] = []
        if account_symbol_key in self.sequence_type:
            del self.sequence_type[account_symbol_key]
        
        # Step 4: Log recovery completed
        self.logger.info(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] Sequence recovery completed successfully")
        
        return True
    
    def determine_trade_sequence(self, symbol, signal_type, account_name):
        """
        Determines the trade sequence based on the event table logic.
        Includes sequence validation and recovery.
        """
        # Get current state
        current_state = self.get_current_state(symbol, account_name)
        if not current_state:
            return {'steps': [], 'position_size': [], 'message': "Could not determine current state"}
        
        current_balance = current_state['balance']
        current_repo = current_state['repo']
        
        # Create compound key for account+symbol
        account_symbol_key = f"{account_name}:{symbol}"
        
        # Check for repeated signals
        if symbol in self.last_signals.get(account_name, {}):
            last_signal = self.last_signals[account_name][symbol]
            last_signal_time = last_signal.get('timestamp', 0)
            current_time = time.time()
            time_diff = current_time - last_signal_time
            
            # Only consider it a repeat if it's within 30 seconds
            if time_diff < 30 and last_signal.get('message') == ('Trend Buy!' if signal_type == 'Buy Signal' else 'Trend Sell!'):
                self.logger.info(f"[{account_name}] Ignoring repeated {signal_type} for {symbol}")
                return {
                    'steps': [],
                    'position_size': [],
                    'message': f"Ignoring repeated {signal_type}"
                }
        
        # Initialize event counter for new symbol
        new_sequence = False
        if account_symbol_key not in self.event_counter:
            new_sequence = True
            if signal_type == 'Buy Signal':
                # Start with buy sequence (1→4→3→4...)
                self.event_counter[account_symbol_key] = 1
                self.sequence_type[account_symbol_key] = "Buy Start"
                self.logger.info(f"[{account_name}] Starting buy sequence (1→4→3→4) for {symbol}")
            elif signal_type == 'Sell Signal':
                # Start with sell sequence (2→3→4→3...)
                self.event_counter[account_symbol_key] = 2
                self.sequence_type[account_symbol_key] = "Sell Start"
                self.logger.info(f"[{account_name}] Starting sell sequence (2→3→4→3) for {symbol}")
        
        current_event = self.event_counter.get(account_symbol_key, 0)
        
        # If not a new sequence, determine next step and validate
        if not new_sequence:
            # Get expected signal based on current event
            expected_signal = None
            if current_event == 1 or current_event == 3:
                expected_signal = "Sell Signal"
            elif current_event == 2 or current_event == 4:
                expected_signal = "Buy Signal"
                
            # Validate sequence - skip if we're just starting
            if current_event > 0:
                is_valid = self.validate_sequence(
                    symbol=symbol,
                    signal_type=signal_type,
                    account_name=account_name,
                    expected_signal=expected_signal
                )
                
                if not is_valid:
                    # If invalid, sequence was reset by recovery, start fresh
                    if signal_type == 'Buy Signal':
                        self.event_counter[account_symbol_key] = 1
                        self.sequence_type[account_symbol_key] = "Buy Start"
                    else:
                        self.event_counter[account_symbol_key] = 2
                        self.sequence_type[account_symbol_key] = "Sell Start"
def get_current_price(self, symbol, account_name):
    """
    Get the current market price for a symbol.
    
    This should come from either:
    1. The price provided in the webhook payload
    2. The price from position client's latest order data
    3. If needed, an external price feed API
    """
    try:
        # First check if we have price data in the position client
        position_client = self.account_manager.get_position_client(account_name)
        if position_client:
            # Try to get the current price from the position client
            # This would typically come from recent trades or market data
            # The implementation depends on the PositionWebsocketClient capabilities
            return position_client.get_latest_price(symbol)
    except Exception as e:
        self.logger.warning(f"Error getting price from position client: {str(e)}")
    
    # If we don't have a price from the position client, log a warning
    self.logger.warning(f"Could not determine market price for {symbol} - webhook should provide price")
    return None  # Return None to indicate price is not available