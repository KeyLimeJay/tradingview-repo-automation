#auto_position_manager.py
#!/usr/bin/env python3
import time
import logging
import threading
from datetime import datetime

class AutoPositionManager:
    """
    Manages automatic position management, including shorting when positions exceed limits.
    Works with multiple accounts and integrates with the trading system.
    """
    def __init__(self, config_manager, account_manager, trading_utils=None):
        self.logger = logging.getLogger("auto_position_manager")
        self.config_manager = config_manager
        self.account_manager = account_manager
        self.trading_utils = trading_utils
        
        # Monitoring thread
        self._monitor_thread = None
        self._stop_event = threading.Event()
        
        # Track recent actions to prevent repeated operations
        self._recent_actions = {}  # {account_name: {symbol: timestamp}}
        
    def start(self):
        """Start the position management monitoring thread"""
        if self._monitor_thread and self._monitor_thread.is_alive():
            self.logger.warning("Position management already running")
            return
            
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop, 
            daemon=True,
            name="AutoPositionManager"
        )
        self._monitor_thread.start()
        self.logger.info("Started automatic position management")
        
    def stop(self):
        """Stop the position management monitoring thread"""
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._stop_event.set()
            self._monitor_thread.join(timeout=5)
            if self._monitor_thread.is_alive():
                self.logger.warning("Position management thread did not terminate cleanly")
            else:
                self.logger.info("Position management stopped")
        else:
            self.logger.info("No position management thread to stop")
            
    def _monitor_loop(self):
        """Main monitoring loop that checks positions across all accounts"""
        self.logger.info("Position management thread started")
        
        while not self._stop_event.is_set():
            try:
                self._check_all_positions()
                
                # Sleep interval based on configuration
                monitor_interval = int(self.config_manager.get_global_setting('position_monitor_interval', 30))
                time.sleep(monitor_interval)
                
            except Exception as e:
                self.logger.error(f"Error in position monitor: {str(e)}")
                time.sleep(10)  # Shorter interval on error
                
    def _check_all_positions(self):
        """Check positions for all accounts and currencies"""
        for account in self.config_manager.get_enabled_accounts():
            account_name = account.get('name')
            
            # Skip if auto shorting is not enabled for this account
            auto_short_enabled = self.config_manager.get_trading_setting(
                account_name, 'auto_short', {}).get('enabled', False)
            if not auto_short_enabled:
                continue
                
            # Get position client for this account
            position_client = self.account_manager.get_position_client(account_name)
            if not position_client:
                self.logger.warning(f"No position client for account {account_name}, skipping checks")
                continue
                
            # Check each trading pair
            for symbol in account.get('trading_pairs', []):
                self._check_position(account_name, symbol, position_client)
                
    def _check_position(self, account_name, symbol, position_client):
        """Check a specific position and take action if needed"""
        try:
            # Force position refresh for accuracy
            position_client.refresh_positions()
            
            # Get current position and limit
            currency = symbol.split('/')[0]
            current_position = position_client.get_truncated_position(symbol)
            strict_limit = self.config_manager.get_currency_setting(
                account_name, currency, 'strict_limit', 0.001)
                
            # Skip if position is below limit
            if current_position < strict_limit:
                return
                
            # Position exceeds limit - check cooldown period
            cooldown = self.config_manager.get_trading_setting(
                account_name, 'auto_short', {}).get('cooldown', 300)
                
            now = time.time()
            if account_name not in self._recent_actions:
                self._recent_actions[account_name] = {}
                
            last_action = self._recent_actions[account_name].get(symbol, 0)
            if now - last_action < cooldown:
                self.logger.debug(
                    f"[{account_name}] Skipping auto-short for {symbol}: "
                    f"cooldown period active ({int(now - last_action)}/{cooldown}s)"
                )
                return
                
            # Proceed with auto-short
            self.logger.warning(
                f"[{account_name}] Position {current_position} exceeds limit {strict_limit} "
                f"for {symbol} - initiating auto-short"
            )
            
            # Get shorting parameters
            short_qty = self.config_manager.get_currency_setting(
                account_name, currency, 'auto_short_quantity', 0.001)
            
            # Execute the short
            success = self._execute_auto_short(account_name, symbol, short_qty)
            
            if success:
                self._recent_actions[account_name][symbol] = now
                self.logger.info(
                    f"[{account_name}] Successfully executed auto-short of {short_qty} {symbol}"
                )
            
        except Exception as e:
            self.logger.error(f"[{account_name}] Error checking position for {symbol}: {str(e)}")
            
    def _execute_auto_short(self, account_name, symbol, quantity):
        """Execute an automatic short order"""
        try:
            # Get account credentials and settings
            credentials = self.config_manager.get_account_credentials(account_name)
            if not credentials:
                self.logger.error(f"No credentials found for account {account_name}")
                return False
                
            # Get API key and secret
            api_key = credentials.get('api_key')
            api_secret = credentials.get('api_secret')
            custodian_id = credentials.get('custodian_id')
            
            if not (api_key and api_secret and custodian_id):
                self.logger.error(f"Missing required credentials for account {account_name}")
                return False
                
            # Get price adjustment for shorts
            price_adj = self.config_manager.get_trading_setting(
                account_name, 'auto_short', {}).get('price_adjustment', 0.95)
                
            # Get current market price (placeholder - in real implementation, 
            # you would fetch the current market price)
            # This would typically come from an external price feed or exchange API
            market_price = self._get_current_price(symbol)
            if not market_price:
                self.logger.error(f"Could not determine market price for {symbol}")
                return False
                
            # Calculate adjusted price
            adjusted_price = market_price * price_adj
            
            # Get TIF setting
            tif = self.config_manager.get_trading_setting(account_name, 'default_tif', 'GTC')
            
            # Execute the order
            if self.trading_utils:
                self.logger.info(
                    f"[{account_name}] Placing auto-short order: {symbol}, "
                    f"quantity={quantity}, price={adjusted_price}"
                )
                
                response = self.trading_utils.place_order(
                    api_key=api_key,
                    api_secret=api_secret,
                    symbol=symbol,
                    side='ASK',  # Sell/short
                    price=adjusted_price,
                    quantity=quantity,
                    custodian_id=custodian_id,
                    tif=tif
                )
                
                if response:
                    return True
                    
                self.logger.error(f"[{account_name}] Failed to place auto-short order")
                return False
            else:
                self.logger.error("Trading utils not available for order placement")
                return False
                
        except Exception as e:
            self.logger.error(f"[{account_name}] Error executing auto-short: {str(e)}")
            return False
            
    def _get_current_price(self, symbol):
        """
        Get current market price for a symbol.
        In a real implementation, this would fetch the latest price from
        an exchange API or market data provider.
        """
        # Placeholder implementation - in real usage, you would replace this
        # with actual price fetching from your trading platform
        
        # Option 1: If you have a real-time price feed
        # return self.price_feed.get_latest_price(symbol)
        
        # Option 2: If you can query the exchange API
        # return self.exchange_api.get_ticker(symbol)['last']
        
        # Option 3: If you can parse recent trades from your own system
        # return self.trading_history.get_last_trade(symbol).price
        
        # For now, we'll just return a placeholder value
        # In the real implementation, you would replace this with actual logic
        try:
            # Try to get last order price from the position websocket if available
            # This is a placeholder and should be replaced with real implementation
            self.logger.warning("Price fetching not implemented - using placeholder")
            return 30000.0  # Placeholder price
        except Exception as e:
            self.logger.error(f"Error fetching current price for {symbol}: {str(e)}")
            return None
            
    def manually_trigger_short(self, account_name, symbol, quantity=None):
        """
        Manually trigger an auto-short operation.
        Useful for testing or recovery operations.
        """
        self.logger.info(f"[{account_name}] Manually triggered auto-short for {symbol}")
        
        position_client = self.account_manager.get_position_client(account_name)
        if not position_client:
            self.logger.error(f"No position client for account {account_name}")
            return False
            
        # If quantity not specified, use the configured auto-short quantity
        if quantity is None:
            currency = symbol.split('/')[0]
            quantity = self.config_manager.get_currency_setting(
                account_name, currency, 'auto_short_quantity', 0.01)
                
        return self._execute_auto_short(account_name, symbol, quantity)