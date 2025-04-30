#!/usr/bin/env python3
from flask import Flask, request, jsonify
import json
import os
import datetime
import logging
from logging.handlers import RotatingFileHandler
import time
from threading import Lock
from collections import OrderedDict
from src.position_websocket import PositionWebsocketClient
from src.trading_utils import (
    place_order, OrderPlacementError, place_repo_order, 
    close_repo, get_jwt_token, get_repo_details
)
from src.account_manager import AccountManager
from src.auto_position_manager import AutoPositionManager
from src.config_manager import ConfigurationManager

class TradingBot:
    """Main trading bot implementation with logic based on event sequence table."""
    
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
        self.event_counter = {}  # symbol -> event_count
        self.sequence_history = {}  # symbol -> [event1, event2, ...]
        
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
        """Verify repo status for a specific account."""
        return self.account_manager.verify_repo_status(symbol, account_name)
    
    def get_current_price(self, symbol, account_name):
        """Get current market price for a symbol (placeholder)"""
        # In a real implementation, you would fetch this from your price feed
        # This is a simplified placeholder
        return 30000.0  # Replace with actual price fetching logic
    
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
            repo_details = get_repo_details(
                symbol=repo_symbol,
                config_manager=self.config_manager,
                account_name=account_name
            )
            if repo_details and 'quantity' in repo_details:
                repo_quantity = repo_details['quantity']
            else:
                # If we can't get the exact quantity, use min_quantity as estimate
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
        if symbol not in self.sequence_history:
            self.sequence_history[symbol] = []
        
        # Get current event
        current_event = self.event_counter.get(symbol, 0)
        
        # Add to history
        self.sequence_history[symbol].append(current_event)
        
        # Only keep the last 10 events to avoid overly long logs
        if len(self.sequence_history[symbol]) > 10:
            self.sequence_history[symbol] = self.sequence_history[symbol][-10:]
        
        # Generate visual representation
        history_str = ' → '.join([str(e) for e in self.sequence_history[symbol]])
        
        # Check for expected patterns in last 4 events
        if len(self.sequence_history[symbol]) >= 4:
            last_four = ''.join(map(str, self.sequence_history[symbol][-4:]))
            # Check for valid patterns
            valid_patterns = ['1434', '4343', '2343', '3434']
            valid_pattern = any(pattern in last_four for pattern in valid_patterns)
            status = "✓ VALID" if valid_pattern else "✗ BROKEN"
        else:
            # Not enough history to validate
            status = "⋯ INITIALIZING"
        
        self.logger.info(f"[SEQUENCE_HISTORY][{account_name}][{symbol}] {history_str} | STATUS: {status}")
    
    def validate_sequence(self, symbol, event_num, signal_type, account_name, 
                         next_expected_event=None, next_expected_signal=None):
        """
        Validate that the sequence is following the expected pattern and trigger recovery if needed
        
        Returns:
            bool: True if sequence is valid, False if broken and recovery needed
        """
        is_valid = True
        warning_message = None
        
        # Check if event number matches what we expect
        if next_expected_event is not None and event_num != next_expected_event:
            is_valid = False
            warning_message = (
                f"[SEQUENCE_BREAK][{symbol}] "
                f"Expected event {next_expected_event}, but got event {event_num}."
            )
        
        # Check if signal type matches what we expect
        if next_expected_signal is not None and signal_type != next_expected_signal:
            is_valid = False
            warning_message = (
                f"[SEQUENCE_BREAK][{symbol}] "
                f"Expected signal {next_expected_signal}, but got {signal_type}."
            )
        
        # Log a warning if validation fails
        if not is_valid and warning_message:
            self.logger.warning(warning_message + " Initiating sequence recovery.")
            # Trigger recovery procedure
            recovery_success = self.recover_sequence(symbol, account_name)
            if recovery_success:
                self.logger.info(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] Recovery successful, sequence reset")
            else:
                self.logger.error(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] Recovery failed")
        
        return is_valid
    
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
            
            success = close_repo(
                symbol=repo_symbol,
                logger=self.logger,
                config_manager=self.config_manager,
                account_name=account_name
            )
            
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
        self.logger.info(f"[SEQUENCE_RECOVERY][{account_name}][{symbol}] Resetting event counter")
        if symbol in self.event_counter:
            self.event_counter[symbol] = 0
        
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
        
        # Check for repeated signals
        if symbol in self.last_signals.get(account_name, {}):
            last_signal = self.last_signals[account_name][symbol]
            last_signal_type = "Buy Signal" if last_signal['message'] == "Trend Buy!" else "Sell Signal"
            
            if last_signal_type == signal_type:
                self.logger.info(f"[{account_name}] Ignoring repeated {signal_type} for {symbol}")
                return {
                    'steps': [],
                    'position_size': [],
                    'message': f"Ignoring repeated {signal_type}"
                }
        
        # Initialize or increment event counter
        if symbol not in self.event_counter:
            # First signal - determine which sequence to follow
            if signal_type == 'Buy Signal' and current_balance == 0 and current_repo == 0:
                # Start with buy sequence (1→4→3→4...)
                self.event_counter[symbol] = 1
                next_expected_event = 4
                next_expected_signal = "Sell Signal"
                self.logger.info(f"[{account_name}] Starting buy sequence for {symbol}")
            elif signal_type == 'Sell Signal' and current_balance == 0 and current_repo == 0:
                # Start with sell sequence (2→3→4→3...)
                self.event_counter[symbol] = 2
                next_expected_event = 3
                next_expected_signal = "Buy Signal"
                self.logger.info(f"[{account_name}] Starting sell sequence for {symbol}")
            else:
                # Default to buy sequence if starting state is unclear
                self.event_counter[symbol] = 1
                next_expected_event = 4
                next_expected_signal = "Sell Signal"
                self.logger.info(f"[{account_name}] Defaulting to buy sequence for {symbol}")
        else:
            # For existing sequences, validate the sequence is not broken
            prev_event = self.event_counter[symbol]
            
            # Determine expected event and signal based on previous event
            if prev_event == 1:
                next_expected_event = 4
                next_expected_signal = "Sell Signal"
            elif prev_event == 2:
                next_expected_event = 3
                next_expected_signal = "Buy Signal"
            elif prev_event == 3:
                next_expected_event = 4
                next_expected_signal = "Sell Signal"
            elif prev_event == 4:
                next_expected_event = 3
                next_expected_signal = "Buy Signal"
            
            # Validate sequence
            is_valid = self.validate_sequence(
                symbol, prev_event + 1, signal_type, 
                account_name, next_expected_event, next_expected_signal
            )
            
            if not is_valid:
                # If sequence was broken and recovery was initiated, we should refresh state
                current_state = self.get_current_state(symbol, account_name)
                current_balance = current_state['balance']
                current_repo = current_state['repo']
            
            # Increment the event counter
            self.event_counter[symbol] += 1
            
            # Handle cycling back from 4 to 3
            if self.event_counter[symbol] > 4:
                self.event_counter[symbol] = 3
        
        event_num = self.event_counter[symbol]
        
        # Log sequence state
        self.log_sequence_state(
            symbol, account_name, event_num, signal_type, 
            current_balance, current_repo
        )
        
        # Log sequence history
        self.log_sequence_history(symbol, account_name)
        
        # Get the unit size for this currency
        base_currency = symbol.split('/')[0]
        unit_size = self.config_manager.get_currency_setting(
            account_name, base_currency, 'min_quantity', 0.001)
        
        # Get repo interest rate from configuration
        repo_interest = float(self.config_manager.get_trading_setting(
            account_name, 'repo_interest_rate', 10.0))
        
        # Create repo details dictionary for reuse
        repo_symbol = f"{base_currency}/USDC110"
        repo_details = {
            'symbol': repo_symbol,
            'quantity': unit_size,
            'interest_rate': repo_interest
        }
        
        # Event table logic
        # Event 1, Buy Signal: Buy
        if event_num == 1 and signal_type == 'Buy Signal':
            return {
                'steps': ['open_long'],
                'position_size': [unit_size],
                'sequential': True,
                'starting_balance': current_balance,
                'starting_repo': current_repo,
                'expected_ending_balance': current_balance + unit_size,
                'expected_ending_repo': current_repo
            }
        
        # Event 2, Sell Signal: Open Repo, Sell
        elif event_num == 2 and signal_type == 'Sell Signal':
            # First part - open repo
            if current_balance == 0 and current_repo == 0:
                return {
                    'steps': ['open_repo', 'open_short'],
                    'position_size': [unit_size, unit_size],
                    'repo_details': repo_details,
                    'sequential': True,
                    'starting_balance': current_balance,
                    'starting_repo': current_repo,
                    'expected_ending_balance': 0,
                    'expected_ending_repo': unit_size
                }
        
        # Event 3, Buy Signal: Buy twice, close repo
        elif event_num == 3 and signal_type == 'Buy Signal':
            # Following event 2 or event 4 - balance at 0, repo at unit_size
            if current_balance == 0 and current_repo == unit_size:
                return {
                    'steps': ['open_long', 'open_long', 'close_repo'],
                    'position_size': [unit_size, unit_size, repo_symbol],
                    'sequential': True,
                    'starting_balance': current_balance,
                    'starting_repo': current_repo,
                    'expected_ending_balance': unit_size,
                    'expected_ending_repo': 0
                }
        
        # Event 4, Sell Signal: Sell, Open Repo, Sell
        elif event_num == 4 and signal_type == 'Sell Signal':
            # Following event 1 or event 3 - balance at 1 or more, repo at 0
            if current_balance >= unit_size and current_repo == 0:
                return {
                    'steps': ['open_short', 'open_repo', 'open_short'],
                    'position_size': [unit_size, unit_size, unit_size],
                    'repo_details': repo_details,
                    'sequential': True,
                    'starting_balance': current_balance,
                    'starting_repo': current_repo,
                    'expected_ending_balance': 0,
                    'expected_ending_repo': unit_size
                }
        
        # For other cases or when the sequence doesn't match the table
        self.logger.warning(f"[{account_name}] No matching table logic for event={event_num}, " +
                           f"signal={signal_type}, balance={current_balance}, repo={current_repo}")
        return {
            'steps': [],
            'position_size': [],
            'message': f"No table logic for event {event_num} with {signal_type}"
        }
    
    def format_price(self, price, symbol, account_name='default'):
        """Format price according to symbol's decimal precision from configuration."""
        try:
            price_float = float(str(price).replace(',', ''))
            if price_float <= 0:
                raise ValueError("Price must be greater than 0")
            
            base_currency = symbol.split('/')[0]
            price_decimals = self.config_manager.get_currency_setting(
                account_name, base_currency, 'price_decimals', 2)
            return round(price_float, price_decimals)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid price value: {price}. Error: {e}")
    
    def webhook(self):
        """Handle incoming webhook requests from TradingView."""
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
            
            # Convert message to signal type for table logic
            signal_type = "Buy Signal" if message == "Trend Buy!" else "Sell Signal"
            
            # Get the position client for this account
            position_client = self.account_manager.get_position_client(account_name)
            if not position_client:
                self.logger.error(f"[{request_id}] No position client for account: {account_name}")
                return jsonify({"success": False, "error": f"No position client for account: {account_name}"}), 500
                
            # Get the account config
            account = self.config_manager.get_account(account_name)
            if not account:
                self.logger.error(f"[{request_id}] No account configuration for: {account_name}")
                return jsonify({"success": False, "error": f"No account configuration for: {account_name}"}), 500
            
            # Initialize account signals tracking if not exists
            if account_name not in self.last_signals:
                self.last_signals[account_name] = {}
            
            # Force position refresh before decision making
            position_client.refresh_positions()
            
            # Get trade sequence based on table logic
            trade_sequence = self.determine_trade_sequence(symbol, signal_type, account_name)
            
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
            
            price = self.format_price(data['price'], symbol, account_name)
            position_sizes = trade_sequence['position_size']
            
            # Dictionary to track currency repo operations for this request
            repo_operations = {}
            
            # Check if sequential execution is required
            sequential_required = trade_sequence.get('sequential', False)
            
            responses = []
            for i, step in enumerate(trade_sequence['steps']):
                try:
                    self.logger.info(f"[{request_id}][{account_name}] Executing step {i+1}: {step}")
                    
                    if step == 'open_long':
                        response = place_order(
                            symbol=symbol,
                            side='BID',
                            price=price,
                            quantity=position_sizes[i],
                            config_manager=self.config_manager,
                            account_name=account_name
                        )
                        
                        if not response and sequential_required:
                            error_msg = f"Step {step} failed. Aborting subsequent steps."
                            self.logger.error(f"[{request_id}][{account_name}] {error_msg}")
                            return jsonify({"success": False, "error": error_msg}), 500
                            
                        responses.append({'step': step, 'response': response})
                        
                        # Position verification after execution
                        time.sleep(1)  # Wait for execution to reflect in positions
                        position_client.refresh_positions()
                        
                    elif step == 'open_short':
                        response = place_order(
                            symbol=symbol,
                            side='ASK',
                            price=price,
                            quantity=position_sizes[i],
                            config_manager=self.config_manager,
                            account_name=account_name
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
                        
                        # Use config-driven repo operations
                        self.logger.info(f"[{request_id}][{account_name}] Using API key authentication for repo operation")
                        response = place_repo_order(
                            symbol=repo_details['symbol'],
                            quantity=repo_details['quantity'],
                            interest_rate=repo_details['interest_rate'],
                            config_manager=self.config_manager,
                            account_name=account_name,
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
                        
                        # Use config-driven repo operations
                        success = close_repo(
                            symbol=repo_symbol,
                            logger=self.logger,
                            config_manager=self.config_manager,
                            account_name=account_name
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
            final_position = self.get_current_state(symbol, account_name)
            
            # Compare expected vs actual ending state
            expected_ending_balance = trade_sequence.get('expected_ending_balance')
            expected_ending_repo = trade_sequence.get('expected_ending_repo')
            actual_ending_balance = final_position['balance']
            actual_ending_repo = final_position['repo']
            
            state_match = (expected_ending_balance == actual_ending_balance and 
                          expected_ending_repo == actual_ending_repo)
            
            if not state_match:
                self.logger.warning(f"[{request_id}][{account_name}] Final state doesn't match expected state:" +
                                  f" Expected bal={expected_ending_balance}, repo={expected_ending_repo}," +
                                  f" Actual bal={actual_ending_balance}, repo={actual_ending_repo}")
            
            response_data = {
                "success": True,
                "account": account_name,
                "orders": responses,
                "message": f"Successfully executed trade sequence: {', '.join(trade_sequence['steps'])}",
                "event": self.event_counter[symbol],
                "starting_balance": trade_sequence.get('starting_balance'),
                "starting_repo": trade_sequence.get('starting_repo'),
                "expected_ending_balance": expected_ending_balance,
                "expected_ending_repo": expected_ending_repo,
                "actual_ending_balance": actual_ending_balance,
                "actual_ending_repo": actual_ending_repo,
                "state_match": state_match
            }
            
            self.logger.info(f"[{request_id}][{account_name}] Trade sequence executed successfully: {json.dumps(responses, indent=2)}")
            self.logger.info(f"[{request_id}][{account_name}] Final state: balance={actual_ending_balance}, repo={actual_ending_repo}")
            return jsonify(response_data), 200
            
        except Exception as e:
            self.logger.error(f"[{request_id}] Unexpected error: {str(e)}")
            return jsonify({"success": False, "error": str(e)}), 500
            
        finally:
            self.webhook_lock.release()

    def get_positions(self):
        """Get current positions for all trading pairs across all accounts."""
        try:
            positions_data = {'accounts': {}}
            
            # Get positions for each account
            for account in self.config_manager.get_enabled_accounts():
                account_name = account.get('name')
                position_client = self.account_manager.get_position_client(account_name)
                if not position_client:
                    continue
                    
                # Force refresh positions before reporting
                position_client.refresh_positions()
                
                # Get trading pairs for this account
                trading_pairs = self.config_manager.get_account_setting(
                    account_name, 'trading_pairs', [])
                
                # Get positions for all trading pairs for this account
                account_positions = {}
                for symbol in trading_pairs:
                    state = self.get_current_state(symbol, account_name)
                    account_positions[symbol] = {
                        'balance': state['balance'],
                        'repo': state['repo'],
                        'event_counter': self.event_counter.get(symbol, 0)
                    }
                
                positions_data['accounts'][account_name] = {
                    'positions': account_positions,
                    'timeframes': self.config_manager.get_account_setting(
                        account_name, 'timeframes', [])
                }
            
            positions_data['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            self.logger.info(f"Position status request: {json.dumps(positions_data, indent=2)}")
            return jsonify(positions_data), 200
            
        except Exception as e:
            error_msg = f"Failed to get positions: {str(e)}"
            self.logger.error(error_msg)
            return jsonify({"success": False, "error": error_msg}), 500

    def health_check(self):
        """Basic health check endpoint for all accounts."""
        health_data = {
            "status": "ok",
            "accounts": {},
            "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "version": self.config_manager.config.get('version', '2.0.0')
        }
        
        # Check health for each account
        for account in self.config_manager.get_enabled_accounts():
            account_name = account.get('name')
            position_client = self.account_manager.get_position_client(account_name)
            if not position_client:
                health_data["accounts"][account_name] = {
                    "status": "error",
                    "error": "No position client available"
                }
                continue
                
            ws_status = "Connected" if position_client.is_connected() else "Disconnected"
            
            # Get trading pairs for this account
            trading_pairs = self.config_manager.get_account_setting(
                account_name, 'trading_pairs', [])
            
            # Get position status for each trading pair
            position_status = {}
            event_status = {}
            for symbol in trading_pairs:
                state = self.get_current_state(symbol, account_name)
                position_status[symbol] = {
                    "balance": state['balance'],
                    "repo": state['repo']
                }
                event_status[symbol] = self.event_counter.get(symbol, 0)
            
            # Add auto-short status
            auto_short_enabled = self.config_manager.get_trading_setting(
                account_name, 'auto_short', {}).get('enabled', False)
            
            # Add account-specific health data
            health_data["accounts"][account_name] = {
                "status": "ok" if ws_status == "Connected" else "warning",
                "websocket": ws_status,
                "position_status": position_status,
                "event_status": event_status,
                "timeframes": self.config_manager.get_account_setting(
                    account_name, 'timeframes', []),
                "auto_short": {
                    "enabled": auto_short_enabled
                }
            }
        
        return jsonify(health_data), 200
    
    def sequence_health(self):
        """Endpoint to check sequence health for all symbols."""
        sequence_data = {}
        
        for symbol in self.event_counter:
            for account in self.config_manager.get_enabled_accounts():
                account_name = account.get('name')
                if symbol in account.get('trading_pairs', []):
                    state = self.get_current_state(symbol, account_name)
                    
                    current_event = self.event_counter[symbol]
                    
                    # Determine sequence type
                    if current_event in [1, 3, 4]:
                        sequence_type = "Buy Start (1→4→3→4)"
                        if current_event == 1:
                            next_expected = 4
                            next_signal = "Sell Signal"
                        elif current_event == 3:
                            next_expected = 4
                            next_signal = "Sell Signal"
                        else:  # event 4
                            next_expected = 3
                            next_signal = "Buy Signal"
                    else:  # event 2
                        sequence_type = "Sell Start (2→3→4→3)"
                        next_expected = 3
                        next_signal = "Buy Signal"
                    
                    # Check sequence validity
                    if symbol in self.sequence_history and len(self.sequence_history[symbol]) >= 4:
                        last_four = ''.join(map(str, self.sequence_history[symbol][-4:]))
                        valid_patterns = ['1434', '4343', '2343', '3434']
                        is_valid = any(pattern in last_four for pattern in valid_patterns)
                    else:
                        is_valid = True  # Not enough history to validate
                    
                    sequence_data[f"{account_name}:{symbol}"] = {
                        "current_event": current_event,
                        "sequence_type": sequence_type,
                        "next_expected_event": next_expected,
                        "next_expected_signal": next_signal,
                        "current_balance": state['balance'],
                        "current_repo": state['repo'],
                        "sequence_valid": is_valid,
                        "sequence_history": self.sequence_history.get(symbol, [])
                    }
        
        return jsonify(sequence_data), 200
        
    def handle_sequence_reset(self):
        """Endpoint to manually reset sequence counters."""
        try:
            data = request.json
            
            # Validate required fields
            if not data:
                return jsonify({
                    "success": False, 
                    "error": "Missing request data"
                }), 400
                
            # Check if we're resetting all symbols or just one
            symbol = data.get('symbol')
            account_name = data.get('account')
            
            if symbol:
                # Reset specific symbol
                if symbol in self.event_counter:
                    old_value = self.event_counter[symbol]
                    self.event_counter[symbol] = 0
                    if symbol in self.sequence_history:
                        self.sequence_history[symbol] = []
                    self.logger.info(f"Reset event counter for {symbol} from {old_value} to 0")
                    return jsonify({
                        "success": True,
                        "message": f"Reset event counter for {symbol} from {old_value} to 0"
                    }), 200
                else:
                    return jsonify({
                        "success": False,
                        "error": f"Symbol {symbol} not found in event counters"
                    }), 404
            else:
                # Reset all symbols
                old_counters = self.event_counter.copy()
                self.event_counter = {}
                self.sequence_history = {}
                self.logger.info(f"Reset all event counters: {old_counters}")
                return jsonify({
                    "success": True,
                    "message": "Reset all event counters",
                    "previous_counters": old_counters
                }), 200
                
        except Exception as e:
            error_msg = f"Error in sequence reset: {str(e)}"
            self.logger.error(error_msg)
            return jsonify({"success": False, "error": error_msg}), 500
        
    def trigger_auto_short(self):
        """Manually trigger auto-short for testing and emergencies."""
        try:
            data = request.json
            
            # Validate required fields
            required_fields = ['account', 'symbol']
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                return jsonify({
                    "success": False, 
                    "error": f"Missing required fields: {missing_fields}"
                }), 400
                
            account_name = data['account']
            symbol = data['symbol']
            quantity = data.get('quantity')  # Optional
            
            # Check if account exists
            if not self.config_manager.get_account(account_name):
                return jsonify({
                    "success": False, 
                    "error": f"Account not found: {account_name}"
                }), 404
                
            # Execute the auto-short
            self.logger.info(f"Manually triggered auto-short for {symbol} on account {account_name}")
            result = self.auto_position_manager.manually_trigger_short(
                account_name, symbol, quantity)
                
            if result:
                return jsonify({
                    "success": True, 
                    "message": f"Auto-short triggered for {symbol}"
                }), 200
            else:
                return jsonify({
                    "success": False, 
                    "error": "Failed to execute auto-short"
                }), 500
                
        except Exception as e:
            error_msg = f"Error in auto-short trigger: {str(e)}"
            self.logger.error(error_msg)
            return jsonify({"success": False, "error": error_msg}), 500

    def reset_event_counter(self, symbol=None):
        """Reset the event counter for a specific symbol or all symbols."""
        if symbol:
            if symbol in self.event_counter:
                self.event_counter[symbol] = 0
                if symbol in self.sequence_history:
                    self.sequence_history[symbol] = []
                self.logger.info(f"Reset event counter for {symbol}")
        else:
            self.event_counter = {}
            self.sequence_history = {}
            self.logger.info("Reset all event counters")
        return True

    def run(self):
        """Run the trading bot with event-based table logic."""
        try:
            # Start all WebSocket clients
            self.account_manager.start_all_clients()
            
            # Give the WebSockets time to connect
            time.sleep(2)
            
            # Start automatic position management
            self.auto_position_manager.start()
            
            # Get port and host from configuration
            port = int(self.config_manager.get_global_setting('port', 6100))
            host = self.config_manager.get_global_setting('host', '0.0.0.0')
            debug = self.config_manager.get_global_setting('environment', 'production') == 'development'
            
            self.logger.info(f"Trading bot with table-based logic starting on {host}:{port}")
            self.logger.info(f"Environment: {'Development' if debug else 'Production'}")
            
            # Log information about all accounts
            for account in self.config_manager.get_enabled_accounts():
                name = account.get('name')
                self.logger.info(f"Account: {name}")
                self.logger.info(f"  Timeframes: {','.join(account.get('timeframes', []))}")
                self.logger.info(f"  Trading pairs: {','.join(account.get('trading_pairs', []))}")
                credentials = self.config_manager.get_account_credentials(name)
                self.logger.info(f"  API URL: {credentials.get('api_url')}")
                self.logger.info(f"  WebSocket URL: {credentials.get('ws_url')}")
                
                # Log auto-short status
                auto_short_enabled = self.config_manager.get_trading_setting(
                    name, 'auto_short', {}).get('enabled', False)
                if auto_short_enabled:
                    self.logger.info(f"  Auto-short: Enabled")
                else:
                    self.logger.info(f"  Auto-short: Disabled")
            
            # Start the Flask application
            self.app.run(host=host, port=port, debug=debug)
        
        except Exception as e:
            self.logger.error(f"Error starting trading bot: {e}")
            raise
        finally:
            self.auto_position_manager.stop()
            self.account_manager.stop_all_clients()

def create_app(config_path=None):
    """Create a new trading bot instance with table-based logic."""
    bot = TradingBot(config_path)
    return bot

if __name__ == "__main__":
    bot = create_app()
    bot.run()