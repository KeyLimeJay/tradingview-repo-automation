#!/usr/bin/env python3
import logging
import os
import time
from src.position_websocket import PositionWebsocketClient
from src.trading_utils import get_jwt_token, get_repo_details

class AccountManager:
    """Manages multiple trading accounts and their position clients."""
    
    def __init__(self, config_manager, logger=None):
        """Initialize the account manager with configuration."""
        self.config_manager = config_manager
        self.logger = logger or logging.getLogger(__name__)
        self.accounts = {}
        self.position_clients = {}
        self.repo_cache = {}
        self.repo_cache_ttl = 30  # TTL in seconds
        self.last_repo_check = {}  # Account -> symbol -> timestamp

    def initialize_accounts(self):
        """Initialize accounts from configuration."""
        self.logger.info("Initializing accounts from configuration")
        account_list = self.config_manager.get_enabled_accounts()
        
        for account_data in account_list:
            account_name = account_data.get('name')
            if account_name:
                self.accounts[account_name] = account_data
                timeframes = account_data.get('timeframes', [])
                trading_pairs = account_data.get('trading_pairs', [])
                
                # Log for debugging
                timeframes_str = ','.join(timeframes) if timeframes else ''
                pairs_str = ','.join(trading_pairs) if trading_pairs else ''
                self.logger.info(f"Added account: {account_name}, Timeframes: {timeframes_str}, Pairs: {pairs_str}")
                
                # Set up timeframe routing
                for timeframe in timeframes:
                    self.logger.info(f"Routing timeframe {timeframe} to account {account_name}")
                    self.config_manager.timeframe_routing[timeframe] = account_name
        
        return len(self.accounts)
    
    def initialize_position_clients(self):
        """Initialize position clients for all accounts."""
        for account_name in self.accounts:
            self.initialize_position_client(account_name)
    
    def initialize_position_client(self, account_name):
        """Initialize position client for a specific account."""
        try:
            if not account_name in self.accounts:
                self.logger.error(f"Account not found: {account_name}")
                return None
                
            account = self.accounts[account_name]
            credentials = self.config_manager.get_account_credentials(account_name)
            
            if not credentials:
                self.logger.error(f"No credentials found for account: {account_name}")
                return None
                
            # Create position client with account info
            client = PositionWebsocketClient(
                api_key=credentials.get('api_key'),
                api_secret=credentials.get('api_secret'),
                logger=self.logger,
                # Pass the account name and config manager
                account_name=account_name,
                config_manager=self.config_manager
            )
            
            # Set up URLs
            client.base_url = credentials.get('api_base_url')
            client.ws_url = credentials.get('ws_url')
            
            # Store in our client dictionary
            self.position_clients[account_name] = client
            self.logger.info(f"Initialized position client for {account_name}")
            
            return client
            
        except Exception as e:
            self.logger.error(f"Failed to initialize position client for {account_name}: {str(e)}")
            return None
    
    def start_all_clients(self):
        """Start all position clients."""
        for account_name, client in self.position_clients.items():
            self.logger.info(f"Starting position client for {account_name}")
            client.start()
    
    def stop_all_clients(self):
        """Stop all position clients."""
        for account_name, client in self.position_clients.items():
            self.logger.info(f"Stopping position client for {account_name}")
            try:
                client.stop()
            except Exception as e:
                self.logger.error(f"Error stopping client for {account_name}: {str(e)}")
    
    def get_position_client(self, account_name):
        """Get the position client for a specific account."""
        return self.position_clients.get(account_name)
    
    def verify_repo_status(self, symbol, account_name):
        """
        Verify if an active repo exists for a symbol using a combination of 
        cache and API checks for reliable results.
        
        Args:
            symbol: Trading pair symbol (e.g., "BTC/USDC")
            account_name: Account name
            
        Returns:
            bool: True if active repo exists
        """
        # Skip verification if invalid inputs
        if not symbol or not account_name:
            self.logger.warning(f"Invalid inputs for repo verification: symbol={symbol}, account={account_name}")
            return False
            
        # Extract base currency from symbol
        if '/' not in symbol:
            self.logger.warning(f"Invalid symbol format for repo verification: {symbol}")
            return False
            
        base_currency = symbol.split('/')[0]
        repo_symbol = f"{base_currency}/USDC110"
        cache_key = f"{account_name}:{repo_symbol}"
        
        # Check if we need a real verification (TTL expired)
        current_time = time.time()
        last_check_time = self.last_repo_check.get(cache_key, 0)
        ttl_expired = current_time - last_check_time > self.repo_cache_ttl
        
        # Get position client
        client = self.get_position_client(account_name)
        
        # If we have a client and can use WebSocket state
        if client and client.is_connected():
            has_repo = client.has_repo(symbol)
            
            # If repo is found via WebSocket, trust it
            # Otherwise, verify with API call if TTL expired
            if has_repo:
                self.repo_cache[cache_key] = True
                self.last_repo_check[cache_key] = current_time
                self.logger.debug(f"Repo found via WebSocket for {symbol} in account {account_name}")
                return True
                
        # If WebSocket didn't confirm repo or is disconnected, 
        # Check API directly if needed
        if ttl_expired:
            try:
                self.logger.debug(f"Checking repo status via API for {symbol} in account {account_name}")
                
                # Get JWT token for API call - ensure account_name is passed
                jwt_token = get_jwt_token(account_name=account_name, config_manager=self.config_manager)
                
                # If we have a token, use API to check repo status
                if jwt_token:
                    repo_details = get_repo_details(
                        jwt_token=jwt_token,
                        symbol=repo_symbol,
                        logger=self.logger,
                        account_name=account_name,  # Ensure account_name is passed
                        config_manager=self.config_manager
                    )
                    
                    # Update cache based on API response
                    has_repo = bool(repo_details and repo_details.get('id'))
                    self.repo_cache[cache_key] = has_repo
                    self.last_repo_check[cache_key] = current_time
                    
                    # If repo found, update WebSocket client state
                    if has_repo and client:
                        client.set_repo_status(symbol, True)
                        self.logger.info(f"Repo found via API for {symbol} in account {account_name}")
                    
                    return has_repo
                else:
                    self.logger.error(f"Failed to get JWT token for account {account_name}")
                    
            except Exception as e:
                self.logger.error(f"Error verifying repo status for {symbol} in account {account_name}: {str(e)}")
                # On error, rely on cached value
    
        # Use cached value if available, otherwise assume no repo
        cached_value = self.repo_cache.get(cache_key, False)
        self.logger.debug(f"Using cached repo status for {symbol} in account {account_name}: {cached_value}")
        return cached_value
    
    def update_repo_status(self, symbol, account_name, status):
        """
        Manually update repo status in cache and client.
        
        Args:
            symbol: Trading pair symbol
            account_name: Account name
            status: True if repo is active, False otherwise
        """
        if not symbol or not account_name:
            return
            
        # Extract base currency from symbol
        if '/' not in symbol:
            return
            
        base_currency = symbol.split('/')[0]
        repo_symbol = f"{base_currency}/USDC110"
        cache_key = f"{account_name}:{repo_symbol}"
        
        # Update cache
        self.repo_cache[cache_key] = status
        self.last_repo_check[cache_key] = time.time()
        
        # Update client if available
        client = self.get_position_client(account_name)
        if client:
            client.set_repo_status(symbol, status)
    
    def get_all_positions(self):
        """Get positions for all accounts."""
        all_positions = {}
        
        for account_name, client in self.position_clients.items():
            if client:
                # Force position refresh
                client.refresh_positions()
                
                # Get positions
                positions = client.get_all_positions()
                
                all_positions[account_name] = positions
                
        return all_positions