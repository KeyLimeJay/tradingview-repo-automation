#trading_bot.py
#!/usr/bin/env python3
from flask import Flask, request, jsonify
import json
import os
import datetime
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from threading import Lock, Thread, Event
import time
import threading
from collections import OrderedDict
from position_websocket import PositionWebsocketClient
from trading_utils import place_order, OrderPlacementError, place_repo_order, close_repo, get_jwt_token, get_repo_details
import requests
from account_manager import AccountManager

load_dotenv()

class TradingBot:
    def __init__(self):
        self.app = Flask(__name__)
        self.webhook_lock = Lock()
        self.logger = self.setup_logging()
        
        # Initialize account manager
        self.account_manager = AccountManager(logger=self.logger)
        self.logger.info("Loading account configurations...")
        num_accounts = self.account_manager.load_accounts()
        self.logger.info(f"Loaded {num_accounts} account configurations")
        
        # Initialize position clients for all accounts
        self.account_manager.initialize_position_clients()
        
        self._request_cache = OrderedDict()
        self.trading_pairs = os.getenv('TRADING_PAIRS', '').split(',')
        
        # Separate last_signal tracking per account
        self.last_signals = {}  # account_name -> symbol -> {message, timeframe, timestamp}
        
        # Signal rate limiting across all accounts
        self._signal_timestamps = {}  # "symbol:message:timeframe" -> timestamp
        
        self.setup_routes()
        
        # Strict position monitoring (now per account)
        self._monitor_stop_event = Event()
        self._monitor_thread = None
        self.enable_strict_position_monitoring()

    def get_trading_config(self, symbol, timeframe='1h'):
        """Get trading configuration for a symbol from environment variables"""
        base_currency = symbol.split('/')[0]
        return {
            'min_quantity': float(os.getenv(f'{base_currency}_MIN_QUANTITY', 0.001)),
            'max_quantity': float(os.getenv(f'{base_currency}_MAX_QUANTITY', 1.0)),
            'price_decimals': int(os.getenv(f'{base_currency}_PRICE_DECIMALS', 2)),
            'default_tif': os.getenv('DEFAULT_TIF', 'GTC'),
            'repo_qty': float(os.getenv(f'{base_currency}_REPO_QTY', 0.001))
        }

    def setup_logging(self):
        if not os.path.exists('logs'):
            os.makedirs('logs')

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        file_handler = RotatingFileHandler(
            'logs/trading_bot.log',
            maxBytes=int(os.getenv('MAX_LOG_BYTES', 10000000)),
            backupCount=int(os.getenv('LOG_BACKUP_COUNT', 5))
        )
        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        logger = logging.getLogger('system')
        logger.setLevel(os.getenv('LOG_LEVEL', 'INFO'))
        logger.handlers = []
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def setup_routes(self):
        self.app.add_url_rule('/webhook', view_func=self.webhook, methods=['POST'])
        self.app.add_url_rule('/positions', view_func=self.get_positions, methods=['GET'])
        self.app.add_url_rule('/health', view_func=self.health_check, methods=['GET'])

    def validate_request_data(self, data):
        """Validate incoming webhook data from TradingView"""
        if not isinstance(data, dict):
            raise ValueError(f"Invalid data format. Expected dict, got {type(data)}")

        required_fields = ['symbol', 'message', 'price']
        missing_fields = [field for field in required_fields if field not in data]
        
        if missing_fields:
            raise ValueError(f"Missing required fields: {missing_fields}")

        if data['symbol'] not in self.trading_pairs:
            self.logger.warning(f"Received signal for unsupported symbol: {data['symbol']}")
            raise ValueError(f"Invalid symbol: {data['symbol']}. Supported symbols: {', '.join(self.trading_pairs)}")

        valid_messages = ['Trend Buy!', 'Trend Sell!']
        if data['message'] not in valid_messages:
            self.logger.warning(f"Received invalid message type: {data['message']}")
            raise ValueError(f"Invalid message format. Expected one of {valid_messages}")
        
        # Handle and validate timeframe
        if 'timeFrame' not in data:
            data['timeFrame'] = os.getenv('DEFAULT_TIMEFRAME', '1h')
            self.logger.info(f"No timeFrame provided, using default: {data['timeFrame']}")
        else:
            valid_timeframes = ['1m', '5m', '15m', '30m', '1h', '4h', '1d']
            if data['timeFrame'] not in valid_timeframes:
                self.logger.warning(f"Received invalid timeFrame: {data['timeFrame']}")
                raise ValueError(f"Invalid timeFrame: {data['timeFrame']}. Expected one of {valid_timeframes}")

        # Get account for this timeframe
        account = self.account_manager.get_account_for_timeframe(data['timeFrame'])
        if not account:
            self.logger.warning(f"No account configured for timeframe: {data['timeFrame']}")
            raise ValueError(f"No account configured for timeframe: {data['timeFrame']}")
            
        # Add account to the data for later use
        data['account'] = account.name

        # Enhanced duplicate signal detection with time-based threshold
        current_time = time.time()
        signal_key = f"{data['symbol']}:{data['message']}:{data['timeFrame']}"
        
        if signal_key in self._signal_timestamps:
            last_time = self._signal_timestamps[signal_key]
            time_diff = current_time - last_time
            
            # Check for repeated signals (exact match) by account
            if account.name not in self.last_signals:
                self.last_signals[account.name] = {}
                
            account_signals = self.last_signals[account.name]
            
            if (data['symbol'] in account_signals and 
                account_signals[data['symbol']].get('message') == data['message'] and
                account_signals[data['symbol']].get('timeFrame') == data['timeFrame']):
                self.logger.warning(f"Rejected repeated {data['message']} signal for {data['symbol']} on {data['timeFrame']} (account: {account.name})")
                raise ValueError(f"Duplicate signal rejected: {data['message']}")
                
            # Reject signals that arrive too quickly (e.g., within 5 seconds)
            min_signal_interval = float(os.getenv('MIN_SIGNAL_INTERVAL', 5.0))
            if time_diff < min_signal_interval:
                self.logger.warning(f"Signal arrived too quickly after previous signal: {time_diff:.2f}s < {min_signal_interval}s")
                raise ValueError(f"Signal throttled: minimum interval is {min_signal_interval}s (received after {time_diff:.2f}s)")
        
        # Track this signal's timestamp
        self._signal_timestamps[signal_key] = current_time
        
        self.logger.info(f"Received valid trading signal: {json.dumps(data, indent=2)}")
        return True

    def determine_trade_side(self, message):
        """Determine trade side from message"""
        if message == 'Trend Buy!':
            return 'BID'
        elif message == 'Trend Sell!':
            return 'ASK'
        raise ValueError(f"Cannot determine trade side from message: {message}")

    def get_strict_limit(self, symbol):
        """Get the strict position limit for a symbol"""
        base_currency = symbol.split('/')[0]
        strict_limits = {
            'BTC': 0.002,  # Must be less than 0.002
            'ETH': 0.02    # Must be less than 0.02
        }
        return strict_limits.get(base_currency, 0.001)  # Default to 0.001 if not specified

    def verify_repo_status(self, symbol, account_name):
        """
        Verify repo status for a specific account
        """
        position_client = self.account_manager.get_position_client(account_name)
        if not position_client:
            self.logger.error(f"No position client for account: {account_name}")
            return False
            
        # Get the account config
        account = self.account_manager.accounts.get(account_name)
        if not account:
            self.logger.error(f"No account configuration for: {account_name}")
            return False
            
        base_currency = symbol.split('/')[0]
        repo_symbol = f"{base_currency}/USDC110"
        
        # Check WebSocket tracking
        ws_repo_status = position_client.has_repo(symbol)
        
        # For critical decisions, try API directly
        try:
            # Setup for this account
            os.environ['API_USERNAME'] = account.api_username
            os.environ['API_PASSWORD'] = account.api_password
            os.environ['API_CODE'] = account.api_code
            os.environ['API_BASE_URL'] = account.api_base_url
            
            jwt_token = get_jwt_token()
            if jwt_token:
                repo_details = get_repo_details(jwt_token, repo_symbol, self.logger)
                api_repo_status = repo_details is not None
                
                # If API check succeeds, trust it over WebSocket
                if ws_repo_status != api_repo_status:
                    self.logger.warning(f"[{account_name}] Repo status mismatch for {symbol}: WebSocket={ws_repo_status}, API={api_repo_status}")
                    # Update WebSocket tracking to match API
                    position_client.set_repo_status(symbol, api_repo_status)
                    return api_repo_status
                    
        except Exception as e:
            # On API error, log but don't change behavior
            self.logger.debug(f"[{account_name}] Error checking repo status via API: {str(e)}")
        
        # If API check fails or isn't available, use WebSocket data
        return ws_repo_status

    def determine_trade_type(self, symbol, side, timeframe='1h', account_name='default'):
        """
        Determines the trade to execute based on current position and signal.
        Updated with the new logic for additional short positions.
        
        SELL SIGNAL LOGIC:
        - If no position (BTC = 0.000, ETH = 0.00):
            1. Open repo
            2. Open short position
        - If long position (BTC > 0, ETH > 0):
            1. Open sell position
            2. Open repo
            3. Open sell position again
            
        BUY SIGNAL LOGIC:
        - If no repo exists:
            1. Open long position (ONLY ONE)
            2. If End Balance Greater or equal than 2 positions, Open Short Position
        - If repo exists:
            1. Open long position
            2. Open long position again
            3. Close repo
            4. If End Balance Greater or equal than 2 positions, Open Short Position
        """
        # Add a short delay to ensure WebSocket data is up-to-date
        time.sleep(0.5)
        
        # Get the position client for this account
        position_client = self.account_manager.get_position_client(account_name)
        if not position_client:
            self.logger.error(f"No position client for account: {account_name}")
            return {'steps': [], 'position_size': [], 'message': f"No position client for account: {account_name}"}
        
        # Force a position refresh before making decisions
        position_client.refresh_positions()
        
        # Get current position and configuration using truncation
        current_position = position_client.get_truncated_position(symbol)
        trading_config = self.get_trading_config(symbol, timeframe)
        base_currency = symbol.split('/')[0]
        repo_symbol = f"{base_currency}/USDC110"
        
        # Get min quantity and repo settings
        min_quantity = trading_config['min_quantity']
        repo_interest = float(os.getenv('REPO_INTEREST_RATE', 10.0))
        
        # Double-check repo status both locally and via API for this account
        has_open_repo = self.verify_repo_status(symbol, account_name)
        
        # Define strict maximum position limits by currency
        strict_limit = self.get_strict_limit(symbol)
        
        # Determine position status
        is_long = current_position > 0
        no_position = current_position == 0
        
        self.logger.info(f"[{account_name}] Position analysis for {symbol}: position={current_position}, " +
                        f"is_long={is_long}, no_position={no_position}, has_repo={has_open_repo}, strict_limit={strict_limit}")
        
        # Check if position exceeds limits
        if current_position >= strict_limit:
            self.logger.warning(f"[{account_name}] Position {current_position} equals or exceeds strict limit {strict_limit} for {symbol}")
            # Allow selling to continue to reduce position
            if side == 'BID':
                self.logger.warning(f"[{account_name}] Buy signal blocked: Position must be less than {strict_limit}")
                return {'steps': [], 'position_size': [], 'message': f"Buy skipped: Position {current_position} exceeds limit {strict_limit}"}
        
        # Create repo details dictionary for reuse
        repo_details = {
            'symbol': repo_symbol,
            'quantity': min_quantity,
            'interest_rate': repo_interest
        }
        
        # SELL SIGNAL LOGIC
        if side == 'ASK':
            if no_position:
                # No position -> Open repo, then open short position (sequential)
                self.logger.info(f"[{account_name}] Sell signal with no position: opening repo and short position")
                return {
                    'steps': ['open_repo', 'open_short'],
                    'position_size': [min_quantity, min_quantity],
                    'repo_details': repo_details,
                    'sequential': True
                }
            elif is_long:
                # Long position -> Open sell, then open repo, then open sell again (sequential)
                self.logger.info(f"[{account_name}] Sell signal with long position: opening sell, repo, then sell again")
                return {
                    'steps': ['open_short', 'open_repo', 'open_short'],
                    'position_size': [min_quantity, min_quantity, min_quantity],
                    'repo_details': repo_details,
                    'sequential': True
                }
            else:
                # Should never happen with our constraints
                self.logger.info(f"[{account_name}] Unexpected state in sell signal - no action taken")
                return {
                    'steps': [],
                    'position_size': []
                }
        
        # BUY SIGNAL LOGIC
        else:  # side == 'BID'
            # Check if buy would exceed limits
            if current_position + min_quantity >= strict_limit:
                self.logger.warning(f"[{account_name}] Buy would push position to or above limit {strict_limit}. Current: {current_position}")
                return {
                    'steps': [],
                    'position_size': [],
                    'message': f"Buy skipped: Would exceed strict limit of {strict_limit}"
                }
                
            if not has_open_repo:
                # No repo exists -> Open long position (ONLY ONE)
                # NEW: Check if balance would be >= 2 positions to add short
                final_position = current_position + min_quantity
                if final_position >= 2 * min_quantity:
                    self.logger.info(f"[{account_name}] Buy signal with no repo: opening long position + short (balance would be {final_position})")
                    return {
                        'steps': ['open_long', 'open_short'],
                        'position_size': [min_quantity, min_quantity],
                        'sequential': True
                    }
                else:
                    self.logger.info(f"[{account_name}] Buy signal with no repo: opening single long position")
                    return {
                        'steps': ['open_long'],
                        'position_size': [min_quantity]
                    }
            else:
                # NEW BUY WITH REPO LOGIC:
                # 1. Open long position
                # 2. Open long position again
                # 3. Close repo
                # 4. If End Balance >= 2 positions, Open Short Position
                
                # Check if combined buys would exceed limit
                if current_position + (min_quantity * 2) >= strict_limit:
                    self.logger.warning(f"[{account_name}] Double buy would exceed limit. Adapting the strategy.")
                    if current_position + min_quantity < strict_limit:
                        # We can do one buy safely
                        final_position = current_position + min_quantity
                        if final_position >= 2 * min_quantity:
                            return {
                                'steps': ['open_long', 'close_repo', 'open_short'],
                                'position_size': [min_quantity, repo_symbol, min_quantity],
                                'repo_details': {'symbol': repo_symbol},
                                'sequential': True
                            }
                        else:
                            return {
                                'steps': ['open_long', 'close_repo'],
                                'position_size': [min_quantity, repo_symbol],
                                'repo_details': {'symbol': repo_symbol},
                                'sequential': True
                            }
                    else:
                        # Can't even do one buy, just close repo
                        return {
                            'steps': ['close_repo'],
                            'position_size': [repo_symbol],
                            'repo_details': {'symbol': repo_symbol}
                        }
                else:
                    # We can safely do both buys with the repo close in between
                    # Then check if final balance would be >= 2 positions to add short
                    final_position = current_position + (min_quantity * 2)
                    if final_position >= 2 * min_quantity:
                        self.logger.info(f"[{account_name}] Buy with repo: open long, open long again, close repo, open short")
                        return {
                            'steps': ['open_long', 'open_long', 'close_repo', 'open_short'],
                            'position_size': [min_quantity, min_quantity, repo_symbol, min_quantity],
                            'repo_details': {'symbol': repo_symbol},
                            'sequential': True
                        }
                    else:
                        self.logger.info(f"[{account_name}] Buy with repo: open long, open long again, close repo")
                        return {
                            'steps': ['open_long', 'open_long', 'close_repo'],
                            'position_size': [min_quantity, min_quantity, repo_symbol],
                            'repo_details': {'symbol': repo_symbol},
                            'sequential': True
                        }

    def format_price(self, price, symbol):
        """Format price according to symbol's decimal precision"""
        try:
            price_float = float(str(price).replace(',', ''))
            if price_float <= 0:
                raise ValueError("Price must be greater than 0")
            
            trading_config = self.get_trading_config(symbol)
            return round(price_float, trading_config['price_decimals'])
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid price value: {price}. Error: {e}")

    def cancel_partially_filled_orders(self, symbol, reversed_side, account_name='default'):
        """Cancel any partially filled orders that might be left in the order book"""
        self.logger.info(f"[{account_name}] Checking for partially filled orders to cancel for {symbol}")
        # Placeholder for actual implementation
        self.logger.info(f"[{account_name}] Would cancel any partially filled {reversed_side} orders for {symbol}")
        return True

    def verify_position_limits(self, symbol, planned_changes, account_name='default', strict_limit=None):
        """
        Verify that planned position changes won't exceed position limits.
        
        Args:
            symbol: Trading pair symbol
            planned_changes: List of operations and quantities [(operation, quantity), ...]
            account_name: Account to check positions for
            strict_limit: Optional override for the strict limit
            
        Returns:
            tuple: (is_safe, message)
        """
        position_client = self.account_manager.get_position_client(account_name)
        if not position_client:
            return (False, f"No position client for account: {account_name}")
            
        current_position = position_client.get_truncated_position(symbol)
        
        if strict_limit is None:
            strict_limit = self.get_strict_limit(symbol)
        
        # Calculate estimated final position
        estimated_position = current_position
        for op, qty in planned_changes:
            if op == 'BID':
                estimated_position += qty
            elif op == 'ASK':
                estimated_position -= qty
        
        # Check if estimated position exceeds limit
        if estimated_position >= strict_limit:
            return (False, f"Planned operations would result in position {estimated_position}, exceeding limit {strict_limit}")
        
        return (True, "Position within limits")

    def webhook(self):
        """Handle incoming webhook requests from TradingView"""
        request_id = str(time.time())
        
        if not self.webhook_lock.acquire(blocking=False):
            self.logger.warning(f"[{request_id}] Request blocked by lock")
            return jsonify({"success": False, "error": "Request already being processed"}), 429
        
        try:
            data = request.json
            self.logger.info(f"[{request_id}] Processing webhook data: {json.dumps(data, indent=2)}")

            try:
                self.validate_request_data(data)
            except ValueError as e:
                self.logger.error(f"[{request_id}] Validation error: {str(e)}")
                return jsonify({"success": False, "error": str(e)}), 400

            symbol = data['symbol']
            message = data['message']
            timeframe = data['timeFrame']
            account_name = data['account']  # Set during validation
            side = self.determine_trade_side(message)
            
            # Get the position client for this account
            position_client = self.account_manager.get_position_client(account_name)
            if not position_client:
                self.logger.error(f"[{request_id}] No position client for account: {account_name}")
                return jsonify({"success": False, "error": f"No position client for account: {account_name}"}), 500
                
            # Get the account config
            account = self.account_manager.accounts.get(account_name)
            if not account:
                self.logger.error(f"[{request_id}] No account configuration for: {account_name}")
                return jsonify({"success": False, "error": f"No account configuration for: {account_name}"}), 500
            
            # Initialize account signals tracking if not exists
            if account_name not in self.last_signals:
                self.last_signals[account_name] = {}
                
            # Cancel any partially filled orders from previous signals if this is a reversal
            if (symbol in self.last_signals[account_name] and 
                self.last_signals[account_name][symbol].get('message') != message):
                reversed_side = 'ASK' if side == 'BID' else 'BID'
                self.cancel_partially_filled_orders(symbol, reversed_side, account_name)
            
            # Force position refresh before decision making
            position_client.refresh_positions()
            
            trade_sequence = self.determine_trade_type(symbol, side, timeframe, account_name)
            
            # Check if trade sequence is empty (e.g., when skipping due to existing position)
            if not trade_sequence['steps']:
                self.logger.info(f"[{request_id}] No trade steps to execute - skipping order placement")
                # Update last signal to prevent repeated processing
                self.last_signals[account_name][symbol] = {
                    'message': message,
                    'timeFrame': timeframe,
                    'timestamp': time.time()
                }
                
                # If there's a message in the trade_sequence, include it in the response
                response_data = {
                    "success": True,
                    "message": trade_sequence.get('message', f"No action needed for {symbol} with {message} signal"),
                    "current_position": position_client.get_truncated_position(symbol),
                    "account": account_name
                }
                return jsonify(response_data), 200
            
            price = self.format_price(data['price'], symbol)
            position_sizes = trade_sequence['position_size']
            
            # Build planned changes for verification
            planned_changes = []
            for i, step in enumerate(trade_sequence['steps']):
                if step == 'open_long':
                    planned_changes.append(('BID', position_sizes[i]))
                elif step == 'open_short':
                    planned_changes.append(('ASK', position_sizes[i]))
            
            # Verify position limits before executing
            if planned_changes:
                is_safe, limit_message = self.verify_position_limits(symbol, planned_changes, account_name)
                if not is_safe:
                    self.logger.warning(f"[{request_id}] {limit_message}")
                    self.last_signals[account_name][symbol] = {
                        'message': message,
                        'timeFrame': timeframe,
                        'timestamp': time.time()
                    }
                    return jsonify({"success": False, "error": limit_message}), 400
            
            # Dictionary to track currency repo operations for this request
            # This ensures we don't try to open more than one repo for the same currency 
            # in a single request, even if logic somehow determines we should
            repo_operations = {}
            
            # Check if sequential execution is required
            sequential_required = trade_sequence.get('sequential', False)
            
            # Temp environment setup for this account's operations
            os.environ['API_KEY'] = account.api_key
            os.environ['API_SECRET'] = account.api_secret
            os.environ['API_USERNAME'] = account.api_username
            os.environ['API_PASSWORD'] = account.api_password
            os.environ['API_CODE'] = account.api_code
            os.environ['API_BASE_URL'] = account.api_base_url
            os.environ['CUSTODIAN_ID'] = account.custodian_id
            
            responses = []
            for i, step in enumerate(trade_sequence['steps']):
                try:
                    self.logger.info(f"[{request_id}][{account_name}] Executing step {i+1}: {step}")
                    
                    if step == 'open_long':
                        response = place_order(
                            api_key=account.api_key,
                            api_secret=account.api_secret,
                            symbol=symbol,
                            side='BID',
                            price=price,
                            quantity=position_sizes[i],
                            custodian_id=account.custodian_id,
                            tif=os.getenv("DEFAULT_TIF", "GTC"),
                            max_retries=int(os.getenv("MAX_RETRIES", 3))
                        )
                        
                        if not response and sequential_required:
                            error_msg = f"Step {step} failed. Aborting subsequent steps."
                            self.logger.error(f"[{request_id}][{account_name}] {error_msg}")
                            return jsonify({"success": False, "error": error_msg}), 500
                            
                        responses.append({'step': step, 'response': response})
                        
                        # Position verification after execution
                        time.sleep(1)  # Wait for execution to reflect in positions
                        position_client.refresh_positions()
                        current_position = position_client.get_truncated_position(symbol)
                        strict_limit = self.get_strict_limit(symbol)
                        
                        if current_position >= strict_limit:
                            self.logger.warning(f"[{request_id}][{account_name}] Position limit reached after {step}: {current_position} >= {strict_limit}")
                            if sequential_required and i < len(trade_sequence['steps']) - 1:
                                self.logger.warning(f"[{request_id}][{account_name}] Skipping remaining steps to avoid exceeding position limits")
                                break  # Skip remaining steps
                        
                    elif step == 'open_short':
                        response = place_order(
                            api_key=account.api_key,
                            api_secret=account.api_secret,
                            symbol=symbol,
                            side='ASK',
                            price=price,
                            quantity=position_sizes[i],
                            custodian_id=account.custodian_id,
                            tif=os.getenv("DEFAULT_TIF", "GTC"),
                            max_retries=int(os.getenv("MAX_RETRIES", 3))
                        )
                        
                        if not response and sequential_required:
                            error_msg = f"Step {step} failed. Aborting subsequent steps."
                            self.logger.error(f"[{request_id}][{account_name}] {error_msg}")
                            return jsonify({"success": False, "error": error_msg}), 500
                            
                        responses.append({'step': step, 'response': response})
                        
                        # Position verification after execution
                        time.sleep(1)  # Wait for execution to reflect in positions
                        position_client.refresh_positions()
                    
                    elif step == 'open_repo':
                        # Check for existing repo to prevent duplicates
                        repo_details = trade_sequence.get('repo_details')
                        if not repo_details:
                            self.logger.error(f"[{request_id}][{account_name}] Missing repo details for open_repo step")
                            if sequential_required:
                                return jsonify({"success": False, "error": "Missing repo details"}), 500
                            continue
                            
                        repo_symbol = repo_details['symbol']
                        base_currency = repo_symbol.split('/')[0]
                        
                        # Check if we already processed a repo operation for this currency in this request
                        if base_currency in repo_operations:
                            self.logger.warning(f"[{request_id}][{account_name}] Already processed repo for {base_currency} in this request, skipping duplicate")
                            responses.append({
                                'step': step, 
                                'skipped': True, 
                                'reason': f'Already processed repo for {base_currency} in this request'
                            })
                            continue
                        
                        # Triple-check repo status using our comprehensive verification
                        if self.verify_repo_status(symbol, account_name):
                            self.logger.warning(f"[{request_id}][{account_name}] Skipping repo open - repo already exists for {symbol}")
                            responses.append({
                                'step': step, 
                                'skipped': True, 
                                'reason': 'Repo already exists'
                            })
                            # Mark this currency as processed to prevent duplicates
                            repo_operations[base_currency] = 'skipped'
                            continue
                        
                        # Use API key method for repo operations
                        self.logger.info(f"[{request_id}][{account_name}] Using API key authentication for repo operation")
                        response = place_repo_order(
                            api_key=account.api_key,
                            api_secret=account.api_secret,
                            symbol=repo_details['symbol'],
                            quantity=repo_details['quantity'],
                            interest_rate=repo_details['interest_rate'],
                            custodian_id=account.custodian_id,
                            logger=self.logger
                        )
                        
                        # Mark this currency as processed to prevent duplicates
                        repo_operations[base_currency] = 'processed'
                        
                        # Check if step failed
                        if not response and sequential_required:
                            error_msg = f"Step {step} failed. Aborting subsequent steps."
                            self.logger.error(f"[{request_id}][{account_name}] {error_msg}")
                            return jsonify({"success": False, "error": error_msg}), 500
                        
                        # Check if the response indicates an existing repo was found
                        if isinstance(response, dict) and response.get('status') == 'skipped' and response.get('reason') == 'repo_exists':
                            self.logger.warning(f"[{request_id}][{account_name}] Repo already exists for {repo_symbol} (direct API check)")
                            responses.append({
                                'step': step, 
                                'skipped': True, 
                                'reason': 'Repo already exists (API verification)'
                            })
                        else:
                            responses.append({'step': step, 'response': response})
                            
                        # Refresh position data after repo operation
                        time.sleep(1)
                        position_client.refresh_positions()
                    
                    elif step == 'close_repo':
                        self.logger.info(f"[{request_id}][{account_name}] Executing step {i+1}: {step}")
                        repo_symbol = position_sizes[i]  # In this case, position_sizes[i] contains the repo symbol
                        base_currency = repo_symbol.split('/')[0]
                        
                        # Check if we already processed a repo operation for this currency in this request
                        if base_currency in repo_operations:
                            self.logger.warning(f"[{request_id}][{account_name}] Already processed repo for {base_currency} in this request, proceeding with caution")
                        
                        # Verify repo exists before trying to close it
                        if not self.verify_repo_status(symbol, account_name):
                            self.logger.warning(f"[{request_id}][{account_name}] No repo exists for {symbol}, skipping close_repo")
                            responses.append({
                                'step': step, 
                                'skipped': True, 
                                'reason': 'No repo exists to close'
                            })
                            continue
                        
                        # Use API key method for repo operations
                        success = close_repo(
                            api_key=account.api_key,
                            api_secret=account.api_secret,
                            symbol=repo_symbol,
                            logger=self.logger
                        )
                        
                        # Mark this currency as processed
                        repo_operations[base_currency] = 'closed'
                        
                        if not success and sequential_required:
                            error_msg = f"Step {step} failed. Aborting subsequent steps."
                            self.logger.error(f"[{request_id}][{account_name}] {error_msg}")
                            return jsonify({"success": False, "error": error_msg}), 500
                            
                        responses.append({'step': step, 'success': success})
                        
                        # Refresh position data after repo operation
                        time.sleep(1)
                        position_client.refresh_positions()
                        
                except OrderPlacementError as e:
                    error_msg = f"Failed at step {step}: {str(e)}"
                    self.logger.error(f"[{request_id}][{account_name}] {error_msg}")
                    
                    if sequential_required:
                        return jsonify({"success": False, "error": error_msg}), 500
                    
                    responses.append({'step': step, 'error': str(e)})
                    
                except Exception as e:
                    error_msg = f"Unexpected error at step {step}: {str(e)}"
                    self.logger.error(f"[{request_id}][{account_name}] {error_msg}")
                    
                    if sequential_required:
                        return jsonify({"success": False, "error": error_msg}), 500
                    
                    responses.append({'step': step, 'error': str(e)})
            
            # Update last signal after successful trade
            self.last_signals[account_name][symbol] = {
                'message': message,
                'timeFrame': timeframe,
                'timestamp': time.time()
            }
            
            # Final position verification
            time.sleep(1)
            position_client.refresh_positions()
            final_position = position_client.get_truncated_position(symbol)
            
            response_data = {
                "success": True,
                "account": account_name,
                "orders": responses,
                "message": f"Successfully executed trade sequence: {', '.join(trade_sequence['steps'])}",
                "final_position": final_position
            }
            
            self.logger.info(f"[{request_id}][{account_name}] Trade sequence executed successfully: {json.dumps(responses, indent=2)}")
            self.logger.info(f"[{request_id}][{account_name}] Final position after trade: {final_position}")
            return jsonify(response_data), 200
            
        except Exception as e:
            self.logger.error(f"[{request_id}] Unexpected error: {str(e)}")
            return jsonify({"success": False, "error": str(e)}), 500
            
        finally:
            self.webhook_lock.release()

    def get_positions(self):
        """Get current positions for all trading pairs across all accounts"""
        try:
            positions_data = {'accounts': {}}
            
            # Get positions for each account
            for account_name, account in self.account_manager.accounts.items():
                position_client = self.account_manager.get_position_client(account_name)
                if not position_client:
                    continue
                    
                # Force refresh positions before reporting
                position_client.refresh_positions()
                
                # Get positions for all trading pairs for this account
                account_positions = {}
                for symbol in account.trading_pairs:
                    account_positions[symbol] = {
                        'raw_quantity': position_client.get_position(symbol),
                        'truncated_quantity': position_client.get_truncated_position(symbol),
                        'has_repo': self.verify_repo_status(symbol, account_name)
                    }
                
                positions_data['accounts'][account_name] = {
                    'positions': account_positions,
                    'timeframes': account.timeframes
                }
            
            positions_data['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            self.logger.info(f"Position status request: {json.dumps(positions_data, indent=2)}")
            return jsonify(positions_data), 200
            
        except Exception as e:
            error_msg = f"Failed to get positions: {str(e)}"
            self.logger.error(error_msg)
            return jsonify({"success": False, "error": error_msg}), 500

    def health_check(self):
        """Basic health check endpoint for all accounts"""
        health_data = {
            "status": "ok",
            "accounts": {},
            "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "version": "2.0.0"  # Updated version for multi-account support
        }
        
        # Check health for each account
        for account_name, account in self.account_manager.accounts.items():
            position_client = self.account_manager.get_position_client(account_name)
            if not position_client:
                health_data["accounts"][account_name] = {
                    "status": "error",
                    "error": "No position client available"
                }
                continue
                
            ws_status = "Connected" if position_client.is_connected() else "Disconnected"
            
            # Get repo status for each trading pair using improved verification
            repo_status = {}
            for symbol in account.trading_pairs:
                base_currency = symbol.split('/')[0]
                repo_symbol = f"{base_currency}/USDC110"
                repo_status[repo_symbol] = self.verify_repo_status(symbol, account_name)
            
            # Add position limit status
            position_status = {}
            for symbol in account.trading_pairs:
                current_position = position_client.get_truncated_position(symbol)
                strict_limit = self.get_strict_limit(symbol)
                position_status[symbol] = {
                    "position": current_position,
                    "limit": strict_limit,
                    "within_limit": current_position < strict_limit
                }
            
            # Add account-specific health data
            health_data["accounts"][account_name] = {
                "status": "ok" if ws_status == "Connected" else "warning",
                "websocket": ws_status,
                "repo_status": repo_status,
                "position_status": position_status,
                "timeframes": account.timeframes
            }
        
        return jsonify(health_data), 200

    def enable_strict_position_monitoring(self):
        """Set up stricter position monitoring for all accounts"""
        # Start a background thread to periodically check positions
        if self._monitor_thread is None or not self._monitor_thread.is_alive():
            self._monitor_stop_event.clear()
            self._monitor_thread = Thread(target=self._position_monitor_loop, daemon=True)
            self._monitor_thread.start()
            self.logger.info("Started strict position monitoring for all accounts")
        else:
            self.logger.info("Position monitoring already running")

    def disable_position_monitoring(self):
        """Disable position monitoring"""
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_stop_event.set()
            self._monitor_thread.join(timeout=5)
            self.logger.info("Position monitoring stopped")
        else:
            self.logger.info("No position monitoring thread to stop")

    def _position_monitor_loop(self):
        """Background loop to monitor positions for limit violations across all accounts"""
        self.logger.info("Position monitor thread started for all accounts")
        
        # Counter for periodic repo verification
        verify_counter = 0
        
        while not self._monitor_stop_event.is_set():
            try:
                # Check all accounts
                for account_name, account in self.account_manager.accounts.items():
                    position_client = self.account_manager.get_position_client(account_name)
                    if not position_client:
                        self.logger.warning(f"No position client for account {account_name}, skipping monitoring")
                        continue
                        
                    # Check all trading pairs for this account
                    for symbol in account.trading_pairs:
                        # Force a refresh before checking
                        position_client.refresh_positions()
                        current_position = position_client.get_truncated_position(symbol)
                        strict_limit = self.get_strict_limit(symbol)
                        
                        if current_position >= strict_limit:
                            self.logger.error(f"[{account_name}] POSITION LIMIT EXCEEDED: {symbol} position {current_position} >= limit {strict_limit}")
                            # Log additional debug information
                            has_repo = self.verify_repo_status(symbol, account_name)
                            self.logger.error(f"[{account_name}] Current state: {symbol} position={current_position}, has_repo={has_repo}")
                        
                        # Check for inconsistent states
                        if current_position == 0 and self.verify_repo_status(symbol, account_name):
                            self.logger.warning(f"[{account_name}] INCONSISTENT STATE: {symbol} has zero position but repo exists")
                
                # Periodic deep repo verification (every 5 cycles, approximately every 2.5 minutes)
                verify_counter += 1
                if verify_counter >= 5:
                    verify_counter = 0
                    self.logger.info("Performing deep repo verification for all accounts")
                    self._verify_all_repos_all_accounts()
                    
                # Sleep for monitoring interval (configurable)
                monitor_interval = int(os.getenv('POSITION_MONITOR_INTERVAL', 30))
                time.sleep(monitor_interval)
                
            except Exception as e:
                self.logger.error(f"Error in position monitor: {str(e)}")
                time.sleep(10)  # Shorter interval on error
                
    def _verify_all_repos_all_accounts(self):
        """Perform deep verification of all repo statuses across all accounts"""
        for account_name, account in self.account_manager.accounts.items():
            try:
                self.logger.info(f"Verifying repos for account: {account_name}")
                
                # Set environment variables for this account
                os.environ['API_USERNAME'] = account.api_username
                os.environ['API_PASSWORD'] = account.api_password
                os.environ['API_CODE'] = account.api_code
                os.environ['API_BASE_URL'] = account.api_base_url
                
                # Get JWT token for API calls
                jwt_token = get_jwt_token()
                if not jwt_token:
                    self.logger.error(f"[{account_name}] Failed to get JWT token for repo verification")
                    continue
                    
                position_client = self.account_manager.get_position_client(account_name)
                if not position_client:
                    self.logger.error(f"[{account_name}] No position client available")
                    continue
                    
                # Check each trading pair
                for symbol in account.trading_pairs:
                    base_currency = symbol.split('/')[0]
                    repo_symbol = f"{base_currency}/USDC110"
                    
                    # Get current WebSocket repo status
                    ws_repo_status = position_client.has_repo(symbol)
                    
                    # Check API directly 
                    try:
                        repo_details = get_repo_details(jwt_token, repo_symbol, self.logger)
                        api_repo_exists = repo_details is not None
                        
                        # If there's a mismatch, correct it
                        if ws_repo_status != api_repo_exists:
                            self.logger.warning(f"[{account_name}] Repo status mismatch for {symbol}: WebSocket={ws_repo_status}, API={api_repo_exists}")
                            # Update WebSocket tracking to match API reality
                            position_client.set_repo_status(symbol, api_repo_exists)
                            self.logger.info(f"[{account_name}] Corrected repo status for {symbol} to {api_repo_exists}")
                    except Exception as e:
                        self.logger.error(f"[{account_name}] Error checking repo details for {repo_symbol}: {str(e)}")
                    
            except Exception as e:
                self.logger.error(f"[{account_name}] Error in deep repo verification: {str(e)}")

    def run(self):
        """Run the trading bot with multi-account support"""
        try:
            # Start all WebSocket clients
            self.account_manager.start_all_clients()
            
            # Give the WebSockets time to connect
            time.sleep(2)
            
            # Enable strict position monitoring
            self.enable_strict_position_monitoring()
            
            port = int(os.getenv('PORT', 6101))
            host = os.getenv('HOST', '0.0.0.0')
            debug = os.getenv('FLASK_ENV') == 'development'
            
            self.logger.info(f"Multi-account trading bot starting on {host}:{port}")
            self.logger.info(f"Environment: {'Development' if debug else 'Production'}")
            
            # Log information about all accounts
            for name, account in self.account_manager.accounts.items():
                self.logger.info(f"Account: {name}")
                self.logger.info(f"  Timeframes: {','.join(account.timeframes)}")
                self.logger.info(f"  Trading pairs: {','.join(account.trading_pairs)}")
                self.logger.info(f"  API URL: {account.api_url}")
                self.logger.info(f"  WebSocket URL: {account.ws_url}")
                self.logger.info(f"  Using Custodian: {account.custodian_id}")
            
            # Start the Flask application
            self.app.run(host=host, port=port, debug=debug)
        
        except Exception as e:
            self.logger.error(f"Error starting trading bot: {e}")
            raise
        finally:
            self.disable_position_monitoring()
            self.account_manager.stop_all_clients()

def create_app():
    """Create a new trading bot instance with multi-account support"""
    bot = TradingBot()
    return bot

if __name__ == '__main__':
    try:
        trading_bot = create_app()
        trading_bot.run()
    except KeyboardInterrupt:
        trading_bot.logger.info("Shutting down trading bot...")
    except Exception as e:
        print(f"Fatal error: {e}")
        raise
                            