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

load_dotenv()

class TradingBot:
    def __init__(self):
        self.app = Flask(__name__)
        self.webhook_lock = Lock()
        self.logger = self.setup_logging()
        self.position_client = PositionWebsocketClient(
            logger=self.logger
        )
        self._request_cache = OrderedDict()
        self.trading_pairs = os.getenv('TRADING_PAIRS', '').split(',')
        self.last_signal = {}  # Track last signal per symbol
        self._signal_timestamps = {}  # Track signal timestamps for rate limiting
        self.setup_routes()
        
        # Strict position monitoring
        self._monitor_stop_event = Event()
        self._monitor_thread = None
        self.enable_strict_position_monitoring()

    def get_trading_config(self, symbol):
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

        # Enhanced duplicate signal detection with time-based threshold
        current_time = time.time()
        signal_key = f"{data['symbol']}:{data['message']}"
        
        if signal_key in self._signal_timestamps:
            last_time = self._signal_timestamps[signal_key]
            time_diff = current_time - last_time
            
            # Check for repeated signals (exact match)
            if data['symbol'] in self.last_signal and self.last_signal[data['symbol']] == data['message']:
                self.logger.warning(f"Rejected repeated {data['message']} signal for {data['symbol']}")
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

    def get_min_quantity(self, symbol):
        """Get the minimum trade quantity for a symbol"""
        base_currency = symbol.split('/')[0]
        min_quantities = {
            'BTC': 0.001,
            'ETH': 0.01
        }
        return min_quantities.get(base_currency, 0.001)  # Default to 0.001 if not specified

    def verify_repo_status(self, symbol):
        """
        Comprehensive repo status verification using multiple sources.
        Returns True if a repo exists, False otherwise.
        """
        base_currency = symbol.split('/')[0]
        repo_symbol = f"{base_currency}/USDC110"
        
        # Check WebSocket tracking
        ws_repo_status = self.position_client.has_repo(symbol)
        
        # For critical decisions, try API directly
        try:
            jwt_token = get_jwt_token()
            if jwt_token:
                repo_details = get_repo_details(jwt_token, repo_symbol, self.logger)
                api_repo_status = repo_details is not None
                
                # If API check succeeds, trust it over WebSocket
                if ws_repo_status != api_repo_status:
                    self.logger.warning(f"Repo status mismatch for {symbol}: WebSocket={ws_repo_status}, API={api_repo_status}")
                    # Update WebSocket tracking to match API
                    self.position_client.set_repo_status(symbol, api_repo_status)
                    return api_repo_status
        except Exception as e:
            # On API error, log but don't change behavior
            self.logger.debug(f"Error checking repo status via API: {str(e)}")
        
        # If API check fails or isn't available, use WebSocket data
        return ws_repo_status

    def _verify_all_repos(self):
        """Perform deep verification of all repo statuses"""
        try:
            # Get JWT token for API calls
            jwt_token = get_jwt_token()
            if not jwt_token:
                self.logger.error("Failed to get JWT token for repo verification")
                return
                
            # Check each trading pair
            for symbol in self.trading_pairs:
                base_currency = symbol.split('/')[0]
                repo_symbol = f"{base_currency}/USDC110"
                
                # Get current WebSocket repo status
                ws_repo_status = self.position_client.has_repo(symbol)
                
                # Check API directly 
                try:
                    repo_details = get_repo_details(jwt_token, repo_symbol, self.logger)
                    api_repo_exists = repo_details is not None
                    
                    # If there's a mismatch, correct it
                    if ws_repo_status != api_repo_exists:
                        self.logger.warning(f"Repo status mismatch for {symbol}: WebSocket={ws_repo_status}, API={api_repo_exists}")
                        # Update WebSocket tracking to match API reality
                        self.position_client.set_repo_status(symbol, api_repo_exists)
                        self.logger.info(f"Corrected repo status for {symbol} to {api_repo_exists}")
                except Exception as e:
                    self.logger.error(f"Error checking repo details for {repo_symbol}: {str(e)}")
                    
        except Exception as e:
            self.logger.error(f"Error in deep repo verification: {str(e)}")

    def determine_trade_type(self, symbol, side):
        """
        Determines the trade to execute based on current position and signal.
        
        For BTC:
        SELL Signal Logic:
        - If BTC Balance is >= 0 and < 0.001:
          * Open repo for 0.001 BTC
          * Open short position for 0.001 BTC
          * If End Balance >= 0.002 BTC, Open Short Position for 0.001 BTC
        - If BTC Balance is > 0.001:
          * Open sell position for 0.001 BTC
          * Open repo for 0.001 BTC
          * Open sell position for 0.001 BTC again
          * If End Balance >= 0.002 BTC, Open Short Position for 0.001 BTC
        
        BUY Signal Logic:
        - If no repo exists for 0.001 BTC:
          * Open long position for 0.001 BTC
          * If End Balance >= 0.002 BTC, Open Short Position for 0.001 BTC
        - If repo exists:
          * Open long position for 0.001 BTC
          * Open long position for 0.001 BTC again
          * Close repo (for 0.001 BTC)
          * If End Balance >= 0.002 BTC, Open Short Position for 0.001 BTC
          
        For ETH:
        SELL Signal Logic:
        - If ETH Balance is >= 0 and < 0.01:
          * Open repo for 0.01 ETH
          * Open short position for 0.01 ETH
          * If End Balance >= 0.02 ETH, Open Short Position for 0.01 ETH
        - If ETH Balance is > 0.01:
          * Open sell position for 0.01 ETH
          * Open repo for 0.01 ETH
          * Open sell position for 0.01 ETH again
          * If End Balance >= 0.02 ETH, Open Short Position for 0.01 ETH
        
        BUY Signal Logic:
        - If no repo exists for 0.01 ETH:
          * Open long position for 0.01 ETH
          * If End Balance >= 0.02 ETH, Open Short Position for 0.01 ETH
        - If repo exists:
          * Open long position for 0.01 ETH
          * Open long position for 0.01 ETH again
          * Close repo (for 0.01 ETH)
          * If End Balance >= 0.02 ETH, Open Short Position for 0.01 ETH
        """
        # Add a short delay to ensure WebSocket data is up-to-date
        time.sleep(0.5)
        
        # Force a position refresh before making decisions
        self.position_client.refresh_positions()
        
        # Get current position and configuration using truncation
        current_position = self.position_client.get_truncated_position(symbol)
        base_currency = symbol.split('/')[0]
        repo_symbol = f"{base_currency}/USDC110"
        
        # Get min quantity based on currency
        min_quantity = self.get_min_quantity(symbol)
        
        # Get repo interest rate
        repo_interest = float(os.getenv('REPO_INTEREST_RATE', 10.0))
        
        # Double-check repo status both locally and via API
        has_open_repo = self.verify_repo_status(symbol)
        
        # Define strict maximum position limits by currency
        strict_limit = self.get_strict_limit(symbol)
        
        # Determine position status based on specific thresholds per currency
        self.logger.info(f"Position analysis for {symbol}: position={current_position}, " +
                        f"has_repo={has_open_repo}, strict_limit={strict_limit}, min_quantity={min_quantity}")
        
        # Check if position exceeds limits
        if current_position >= strict_limit:
            self.logger.warning(f"Position {current_position} equals or exceeds strict limit {strict_limit} for {symbol}")
            # Allow selling to continue to reduce position
            if side == 'BID':
                self.logger.warning(f"Buy signal blocked: Position must be less than {strict_limit}")
                return {'steps': [], 'position_size': [], 'message': f"Buy skipped: Position {current_position} exceeds limit {strict_limit}"}
        
        # Create repo details dictionary for reuse
        repo_details = {
            'symbol': repo_symbol,
            'quantity': min_quantity,
            'interest_rate': repo_interest
        }
        
        # SELL SIGNAL LOGIC
        if side == 'ASK':
            if current_position >= 0 and current_position < min_quantity:
                # Balance is >= 0 and below min_quantity (e.g., < 0.001 BTC or < 0.01 ETH)
                steps = ['open_repo', 'open_short']
                position_sizes = [min_quantity, min_quantity]
                
                # Check if final position would be >= threshold for additional short
                final_position_estimate = abs(current_position - min_quantity)
                if final_position_estimate >= strict_limit:
                    steps.append('open_short')
                    position_sizes.append(min_quantity)
                    self.logger.info(f"Sell signal with small balance: adding extra short as final position ({final_position_estimate}) would exceed threshold")
                
                self.logger.info(f"Sell signal with small balance: {steps}")
                return {
                    'steps': steps,
                    'position_size': position_sizes,
                    'repo_details': repo_details,
                    'sequential': True
                }
            elif current_position >= min_quantity:
                # Balance is above min_quantity (e.g., > 0.001 BTC or > 0.01 ETH)
                steps = ['open_short', 'open_repo', 'open_short']
                position_sizes = [min_quantity, min_quantity, min_quantity]
                
                # Check if final position would be >= threshold for additional short
                final_position_estimate = abs(current_position - (min_quantity * 2))
                if final_position_estimate >= strict_limit:
                    steps.append('open_short')
                    position_sizes.append(min_quantity)
                    self.logger.info(f"Sell signal with large balance: adding extra short as final position ({final_position_estimate}) would exceed threshold")
                
                self.logger.info(f"Sell signal with substantial balance: {steps}")
                return {
                    'steps': steps,
                    'position_size': position_sizes,
                    'repo_details': repo_details,
                    'sequential': True
                }
            else:
                # Negative position (shouldn't happen with our constraints)
                self.logger.info(f"Unexpected state in sell signal - no action taken")
                return {
                    'steps': [],
                    'position_size': []
                }
        
        # BUY SIGNAL LOGIC
        else:  # side == 'BID'
            # Check if buy would exceed limits
            if current_position + min_quantity >= strict_limit:
                self.logger.warning(f"Buy would push position to or above limit {strict_limit}. Current: {current_position}")
                return {
                    'steps': [],
                    'position_size': [],
                    'message': f"Buy skipped: Would exceed strict limit of {strict_limit}"
                }
                
            if not has_open_repo:
                # No repo exists
                steps = ['open_long']
                position_sizes = [min_quantity]
                
                # Check if final balance would be >= threshold for additional short
                final_position_estimate = current_position + min_quantity
                if final_position_estimate >= strict_limit:
                    steps.append('open_short')
                    position_sizes.append(min_quantity)
                    self.logger.info(f"Buy signal with no repo: adding short as final position ({final_position_estimate}) would exceed threshold")
                
                self.logger.info(f"Buy signal with no repo: {steps}")
                return {
                    'steps': steps,
                    'position_size': position_sizes,
                    'sequential': True
                }
            else:
                # Repo exists
                # Check if combined buys would exceed limit
                if current_position + (min_quantity * 2) >= strict_limit:
                    self.logger.warning(f"Double buy would exceed limit. Adapting the strategy.")
                    
                    if current_position + min_quantity < strict_limit:
                        # We can do one buy safely
                        steps = ['open_long', 'close_repo']
                        position_sizes = [min_quantity, repo_symbol]
                        
                        # Check if final balance would be >= threshold for additional short
                        final_position_estimate = current_position + min_quantity
                        if final_position_estimate >= strict_limit:
                            steps.append('open_short')
                            position_sizes.append(min_quantity)
                            self.logger.info(f"Buy with repo (adapted): adding short as final position ({final_position_estimate}) would exceed threshold")
                        
                        self.logger.info(f"Buy signal with repo (adapted for limits): {steps}")
                        return {
                            'steps': steps,
                            'position_size': position_sizes,
                            'repo_details': {'symbol': repo_symbol},
                            'sequential': True
                        }
                    else:
                        # Can't even do one buy, just close repo
                        self.logger.info(f"Buy signal with repo: only closing repo due to position limits")
                        return {
                            'steps': ['close_repo'],
                            'position_size': [repo_symbol],
                            'repo_details': {'symbol': repo_symbol}
                        }
                else:
                    # We can safely do both buys
                    steps = ['open_long', 'open_long', 'close_repo']
                    position_sizes = [min_quantity, min_quantity, repo_symbol]
                    
                    # Check if final balance would be >= threshold for additional short
                    final_position_estimate = current_position + (min_quantity * 2)
                    if final_position_estimate >= strict_limit:
                        steps.append('open_short')
                        position_sizes.append(min_quantity)
                        self.logger.info(f"Buy with repo: adding short as final position ({final_position_estimate}) would exceed threshold")
                    
                    self.logger.info(f"Buy signal with repo: {steps}")
                    return {
                        'steps': steps,
                        'position_size': position_sizes,
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

    def cancel_partially_filled_orders(self, symbol, reversed_side):
        """Cancel any partially filled orders that might be left in the order book"""
        try:
            # Logic to find and cancel partially filled orders
            # This would need to be implemented based on your API capabilities
            # For example, get open orders for the symbol and cancel them if they're small
            self.logger.info(f"Checking for partially filled orders to cancel for {symbol}")
            
            # Placeholder for actual implementation
            # This might involve:
            # 1. Getting open orders for the symbol
            # 2. Checking if any are partially filled and below min threshold
            # 3. Cancelling those orders
            
            # For now, we'll log that this would happen
            self.logger.info(f"Would cancel any partially filled {reversed_side} orders for {symbol}")
            return True
        except Exception as e:
            self.logger.error(f"Error cancelling partially filled orders: {str(e)}")
            return False

    def verify_position_limits(self, symbol, planned_changes, strict_limit=None):
        """
        Verify that planned position changes won't exceed position limits.
        
        Args:
            symbol: Trading pair symbol
            planned_changes: List of operations and quantities [(operation, quantity), ...]
            strict_limit: Optional override for the strict limit
            
        Returns:
            tuple: (is_safe, message)
        """
        current_position = self.position_client.get_truncated_position(symbol)
        
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
            side = self.determine_trade_side(message)
            
            # Cancel any partially filled orders from previous signals if this is a reversal
            if symbol in self.last_signal and self.last_signal[symbol] != message:
                reversed_side = 'ASK' if side == 'BID' else 'BID'
                self.cancel_partially_filled_orders(symbol, reversed_side)
            
            # Force position refresh before decision making
            self.position_client.refresh_positions()
            
            trade_sequence = self.determine_trade_type(symbol, side)
            
            # Check if trade sequence is empty (e.g., when skipping due to existing position)
            if not trade_sequence['steps']:
                self.logger.info(f"[{request_id}] No trade steps to execute - skipping order placement")
                # Update last signal to prevent repeated processing
                self.last_signal[symbol] = message
                
                # If there's a message in the trade_sequence, include it in the response
                response_data = {
                    "success": True,
                    "message": trade_sequence.get('message', f"No action needed for {symbol} with {message} signal"),
                    "current_position": self.position_client.get_truncated_position(symbol)
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
                is_safe, limit_message = self.verify_position_limits(symbol, planned_changes)
                if not is_safe:
                    self.logger.warning(f"[{request_id}] {limit_message}")
                    self.last_signal[symbol] = message  # Still update last signal to prevent duplicates
                    return jsonify({"success": False, "error": limit_message}), 400
            
            # Dictionary to track currency repo operations for this request
            # This ensures we don't try to open more than one repo for the same currency 
            # in a single request, even if logic somehow determines we should
            repo_operations = {}
            
            # Check if sequential execution is required
            sequential_required = trade_sequence.get('sequential', False)
            
            responses = []
            for i, step in enumerate(trade_sequence['steps']):
                try:
                    self.logger.info(f"[{request_id}] Executing step {i+1}: {step}")
                    
                    if step == 'open_long':
                        response = place_order(
                            api_key=os.getenv("API_KEY"),
                            api_secret=os.getenv("API_SECRET"),
                            symbol=symbol,
                            side='BID',
                            price=price,
                            quantity=position_sizes[i],
                            custodian_id=os.getenv("CUSTODIAN_ID"),
                            tif=os.getenv("DEFAULT_TIF", "GTC"),
                            max_retries=int(os.getenv("MAX_RETRIES", 3))
                        )
                        
                        if not response and sequential_required:
                            error_msg = f"Step {step} failed. Aborting subsequent steps."
                            self.logger.error(f"[{request_id}] {error_msg}")
                            return jsonify({"success": False, "error": error_msg}), 500
                            
                        responses.append({'step': step, 'response': response})
                        
                        # Position verification after execution
                        time.sleep(1)  # Wait for execution to reflect in positions
                        self.position_client.refresh_positions()
                        current_position = self.position_client.get_truncated_position(symbol)
                        strict_limit = self.get_strict_limit(symbol)
                        
                        if current_position >= strict_limit:
                            self.logger.warning(f"[{request_id}] Position limit reached after {step}: {current_position} >= {strict_limit}")
                            if sequential_required and i < len(trade_sequence['steps']) - 1:
                                self.logger.warning(f"[{request_id}] Skipping remaining steps to avoid exceeding position limits")
                                break  # Skip remaining steps
                        
                    elif step == 'open_short':
                        response = place_order(
                            api_key=os.getenv("API_KEY"),
                            api_secret=os.getenv("API_SECRET"),
                            symbol=symbol,
                            side='ASK',
                            price=price,
                            quantity=position_sizes[i],
                            custodian_id=os.getenv("CUSTODIAN_ID"),
                            tif=os.getenv("DEFAULT_TIF", "GTC"),
                            max_retries=int(os.getenv("MAX_RETRIES", 3))
                        )
                        
                        if not response and sequential_required:
                            error_msg = f"Step {step} failed. Aborting subsequent steps."
                            self.logger.error(f"[{request_id}] {error_msg}")
                            return jsonify({"success": False, "error": error_msg}), 500
                            
                        responses.append({'step': step, 'response': response})
                        
                        # Position verification after execution
                        time.sleep(1)  # Wait for execution to reflect in positions
                        self.position_client.refresh_positions()
                    
                    elif step == 'open_repo':
                        # Check for existing repo to prevent duplicates
                        repo_details = trade_sequence.get('repo_details')
                        if not repo_details:
                            self.logger.error(f"[{request_id}] Missing repo details for open_repo step")
                            if sequential_required:
                                return jsonify({"success": False, "error": "Missing repo details"}), 500
                            continue
                            
                        repo_symbol = repo_details['symbol']
                        base_currency = repo_symbol.split('/')[0]
                        
                        # Check if we already processed a repo operation for this currency in this request
                        if base_currency in repo_operations:
                            self.logger.warning(f"[{request_id}] Already processed repo for {base_currency} in this request, skipping duplicate")
                            responses.append({
                                'step': step, 
                                'skipped': True, 
                                'reason': f'Already processed repo for {base_currency} in this request'
                            })
                            continue
                        
                        # Triple-check repo status using our comprehensive verification
                        if self.verify_repo_status(symbol):
                            self.logger.warning(f"[{request_id}] Skipping repo open - repo already exists for {symbol}")
                            responses.append({
                                'step': step, 
                                'skipped': True, 
                                'reason': 'Repo already exists'
                            })
                            # Mark this currency as processed to prevent duplicates
                            repo_operations[base_currency] = 'skipped'
                            continue
                        
                        # Use API key method for repo operations
                        self.logger.info(f"[{request_id}] Using API key authentication for repo operation")

                        response = place_repo_order(
                            api_key=os.getenv("API_KEY"),
                            api_secret=os.getenv("API_SECRET"),
                            symbol=repo_details['symbol'],
                            quantity=repo_details['quantity'],
                            interest_rate=repo_details['interest_rate'],
                            custodian_id=os.getenv("CUSTODIAN_ID"),
                            logger=self.logger
                        )
                        
                        # Mark this currency as processed to prevent duplicates
                        repo_operations[base_currency] = 'processed'
                        
                        # Check if step failed
                        if not response and sequential_required:
                            error_msg = f"Step {step} failed. Aborting subsequent steps."
                            self.logger.error(f"[{request_id}] {error_msg}")
                            return jsonify({"success": False, "error": error_msg}), 500
                        
                        # Check if the response indicates an existing repo was found
                        if isinstance(response, dict) and response.get('status') == 'skipped' and response.get('reason') == 'repo_exists':
                            self.logger.warning(f"[{request_id}] Repo already exists for {repo_symbol} (direct API check)")
                            responses.append({
                                'step': step, 
                                'skipped': True, 
                                'reason': 'Repo already exists (API verification)'
                            })
                        else:
                            responses.append({'step': step, 'response': response})
                            
                        # Refresh position data after repo operation
                        time.sleep(1)
                        self.position_client.refresh_positions()
                    
                    elif step == 'close_repo':
                        self.logger.info(f"[{request_id}] Executing step {i+1}: {step}")
                        repo_symbol = position_sizes[i]  # In this case, position_sizes[i] contains the repo symbol
                        base_currency = repo_symbol.split('/')[0]
                        
                        # Check if we already processed a repo operation for this currency in this request
                        if base_currency in repo_operations:
                            self.logger.warning(f"[{request_id}] Already processed repo for {base_currency} in this request, proceeding with caution")
                        
                        # Verify repo exists before trying to close it
                        if not self.verify_repo_status(symbol):
                            self.logger.warning(f"[{request_id}] No repo exists for {symbol}, skipping close_repo")
                            responses.append({
                                'step': step, 
                                'skipped': True, 
                                'reason': 'No repo exists to close'
                            })
                            continue
                        
                        # Use API key method for repo operations
                        success = close_repo(
                            api_key=os.getenv("API_KEY"),
                            api_secret=os.getenv("API_SECRET"),
                            symbol=repo_symbol,
                            logger=self.logger
                        )
                        
                        # Mark this currency as processed
                        repo_operations[base_currency] = 'closed'
                        
                        if not success and sequential_required:
                            error_msg = f"Step {step} failed. Aborting subsequent steps."
                            self.logger.error(f"[{request_id}] {error_msg}")
                            return jsonify({"success": False, "error": error_msg}), 500
                            
                        responses.append({'step': step, 'success': success})
                        
                        # Refresh position data after repo operation
                        time.sleep(1)
                        self.position_client.refresh_positions()
                        
                except OrderPlacementError as e:
                    error_msg = f"Failed at step {step}: {str(e)}"
                    self.logger.error(f"[{request_id}] {error_msg}")
                    
                    if sequential_required:
                        return jsonify({"success": False, "error": error_msg}), 500
                    
                    responses.append({'step': step, 'error': str(e)})
                    
                except Exception as e:
                    error_msg = f"Unexpected error at step {step}: {str(e)}"
                    self.logger.error(f"[{request_id}] {error_msg}")
                    
                    if sequential_required:
                        return jsonify({"success": False, "error": error_msg}), 500
                    
                    responses.append({'step': step, 'error': str(e)})
            
            # Update last signal after successful trade
            self.last_signal[symbol] = message
            
            # Final position verification
            time.sleep(1)
            self.position_client.refresh_positions()
            final_position = self.position_client.get_truncated_position(symbol)
            
            response_data = {
                "success": True,
                "orders": responses,
                "message": f"Successfully executed trade sequence: {', '.join(trade_sequence['steps'])}",
                "final_position": final_position
            }
            
            self.logger.info(f"[{request_id}] Trade sequence executed successfully: {json.dumps(responses, indent=2)}")
            self.logger.info(f"[{request_id}] Final position after trade: {final_position}")
            return jsonify(response_data), 200
            
        except Exception as e:
            self.logger.error(f"[{request_id}] Unexpected error: {str(e)}")
            return jsonify({"success": False, "error": str(e)}), 500
            
        finally:
            self.webhook_lock.release()
            
    def get_positions(self):
        """Get current positions for all trading pairs"""
        try:
            # Force refresh positions before reporting
            self.position_client.refresh_positions()
            
            positions_data = {
                'positions': {
                    symbol: {
                        'raw_quantity': self.position_client.get_position(symbol),
                        'truncated_quantity': self.position_client.get_truncated_position(symbol),
                        'has_repo': self.verify_repo_status(symbol)  # Use improved repo verification
                    }
                    for symbol in self.trading_pairs
                },
                'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            self.logger.info(f"Position status request: {json.dumps(positions_data, indent=2)}")
            return jsonify(positions_data), 200
            
        except Exception as e:
            error_msg = f"Failed to get positions: {str(e)}"
            self.logger.error(error_msg)
            return jsonify({"success": False, "error": error_msg}), 500

    def health_check(self):
        """Basic health check endpoint"""
        ws_status = "Connected" if self.position_client.is_connected() else "Disconnected"
        
        # Get repo status for each trading pair using improved verification
        repo_status = {}
        for symbol in self.trading_pairs:
            base_currency = symbol.split('/')[0]
            repo_symbol = f"{base_currency}/USDC110"
            repo_status[repo_symbol] = self.verify_repo_status(symbol)
        
        # Add position limit status
        position_status = {}
        for symbol in self.trading_pairs:
            current_position = self.position_client.get_truncated_position(symbol)
            strict_limit = self.get_strict_limit(symbol)
            position_status[symbol] = {
                "position": current_position,
                "limit": strict_limit,
                "within_limit": current_position < strict_limit
            }
        
        return jsonify({
            "status": "ok",
            "websocket": ws_status,
            "repo_status": repo_status,
            "position_status": position_status,
            "timestamp": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "version": "1.3.0"  # Updated version to reflect improved currency-specific strategy implementation
        }), 200
        
    def enable_strict_position_monitoring(self):
        """Set up stricter position monitoring"""
        # Start a background thread to periodically check positions
        if self._monitor_thread is None or not self._monitor_thread.is_alive():
            self._monitor_stop_event.clear()
            self._monitor_thread = Thread(target=self._position_monitor_loop, daemon=True)
            self._monitor_thread.start()
            self.logger.info("Started strict position monitoring")
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
        """Background loop to monitor positions for limit violations"""
        self.logger.info("Position monitor thread started")
        
        # Counter for periodic repo verification
        verify_counter = 0
        
        while not self._monitor_stop_event.is_set():
            try:
                # Check all positions
                for symbol in self.trading_pairs:
                    # Force a refresh before checking
                    self.position_client.refresh_positions()
                    current_position = self.position_client.get_truncated_position(symbol)
                    strict_limit = self.get_strict_limit(symbol)
                    
                    if current_position >= strict_limit:
                        self.logger.error(f"POSITION LIMIT EXCEEDED: {symbol} position {current_position} >= limit {strict_limit}")
                        # Log additional debug information
                        has_repo = self.verify_repo_status(symbol)
                        self.logger.error(f"Current state: {symbol} position={current_position}, has_repo={has_repo}")
                    
                    # Check for inconsistent states
                    if current_position == 0 and self.verify_repo_status(symbol):
                        self.logger.warning(f"INCONSISTENT STATE: {symbol} has zero position but repo exists")
                
                # Periodic deep repo verification (every 5 cycles, approximately every 2.5 minutes)
                verify_counter += 1
                if verify_counter >= 5:
                    verify_counter = 0
                    self.logger.info("Performing deep repo verification")
                    self._verify_all_repos()
                    
                # Sleep for monitoring interval (configurable)
                monitor_interval = int(os.getenv('POSITION_MONITOR_INTERVAL', 30))
                time.sleep(monitor_interval)
                
            except Exception as e:
                self.logger.error(f"Error in position monitor: {str(e)}")
                time.sleep(10)  # Shorter interval on error

    def run(self):
        """Run the trading bot"""
        try:
            # Start the WebSocket client
            self.position_client.start()
            
            # Give the WebSocket time to connect
            time.sleep(2)
            
            # Enable strict position monitoring
            self.enable_strict_position_monitoring()
            
            port = int(os.getenv('PORT', 6101))
            host = os.getenv('HOST', '0.0.0.0')
            debug = os.getenv('FLASK_ENV') == 'development'
            
            self.logger.info(f"Trading bot starting on {host}:{port}")
            self.logger.info(f"Environment: {'Development' if debug else 'Production'}")
            self.logger.info(f"Available pairs: {self.trading_pairs}")
            self.logger.info(f"Position limits: BTC={self.get_strict_limit('BTC/USDC')}, ETH={self.get_strict_limit('ETH/USDC')}")
            
            # Print status of env variables
            self.logger.info(f"API URL: {os.getenv('API_URL')}")
            self.logger.info(f"WebSocket URL: {os.getenv('WS_URL')}")
            self.logger.info(f"Using Custodian: {os.getenv('CUSTODIAN_ID')}")
            
            self.app.run(host=host, port=port, debug=debug)
      
        except Exception as e:
            self.logger.error(f"Error starting trading bot: {e}")
            raise
        finally:
            self.disable_position_monitoring()
            self.position_client.stop()

def create_app():
    """Create a new trading bot instance"""
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