#account_manager.py
#!/usr/bin/env python3
import os
import logging
import time
from src.position_websocket import PositionWebsocketClient

class AccountManager:
    """
    Manages multiple trading accounts with different configurations.
    Works with the new configuration system to support flexible account setups.
    """
    def __init__(self, config_manager, logger=None):
        self.logger = logger or logging.getLogger('account_manager')
        self.config_manager = config_manager
        self.accounts = {}  # name -> account config dict
        self.position_clients = {}  # name -> PositionWebsocketClient
        self.timeframe_routing = {}  # timeframe -> account_name
        
    def initialize_accounts(self):
        """Initialize accounts from configuration"""
        self.logger.info("Initializing accounts from configuration")
        accounts_loaded = 0
        
        # Get all enabled accounts from config
        enabled_accounts = self.config_manager.get_enabled_accounts()
        for account_config in enabled_accounts:
            account_name = account_config.get('name')
            if not account_name:
                self.logger.warning("Skipping account with no name")
                continue
                
            credentials = self.config_manager.get_account_credentials(account_name)
            if not credentials:
                self.logger.error(f"No credentials found for account: {account_name}")
                continue
                
            # Combine account config with credentials for complete settings
            complete_config = {
                'name': account_name,
                'api_key': credentials.get('api_key'),
                'api_secret': credentials.get('api_secret'),
                'api_username': credentials.get('api_username'),
                'api_password': credentials.get('api_password'),
                'api_code': credentials.get('api_code'),
                'api_url': credentials.get('api_url'),
                'api_base_url': credentials.get('api_base_url'),
                'ws_url': credentials.get('ws_url'),
                'custodian_id': credentials.get('custodian_id'),
                'trading_pairs': account_config.get('trading_pairs', []),
                'timeframes': account_config.get('timeframes', [])
            }
            
            # Add account if we have the necessary credentials
            if (complete_config['api_key'] and complete_config['api_secret'] and
                complete_config['api_base_url'] and complete_config['ws_url']):
                self.accounts[account_name] = complete_config
                self.logger.info(f"Added account: {account_name}, "
                               f"Timeframes: {','.join(complete_config['timeframes'])}, "
                               f"Pairs: {','.join(complete_config['trading_pairs'])}")
                accounts_loaded += 1
            else:
                self.logger.error(f"Missing required credentials for account: {account_name}")
        
        self.setup_timeframe_routing()
        return accounts_loaded
        
    def setup_timeframe_routing(self):
        """Set up routing from timeframes to accounts"""
        # Clear existing routing
        self.timeframe_routing = {}
        
        for name, account in self.accounts.items():
            for timeframe in account['timeframes']:
                timeframe = timeframe.strip()
                if not timeframe:
                    continue
                    
                if timeframe in self.timeframe_routing:
                    self.logger.warning(
                        f"Timeframe {timeframe} already routed to {self.timeframe_routing[timeframe]}. "
                        f"Overriding with {name}."
                    )
                    
                self.timeframe_routing[timeframe] = name
                self.logger.info(f"Routing timeframe {timeframe} to account {name}")
                
    def get_account_for_timeframe(self, timeframe):
        """Get the account that handles a specific timeframe"""
        if timeframe in self.timeframe_routing:
            account_name = self.timeframe_routing[timeframe]
            return self.accounts.get(account_name)
            
        # If no specific account for this timeframe, try the default
        return self.accounts.get('default')
        
    def initialize_position_clients(self):
        """Initialize position clients for all accounts"""
        for name, account in self.accounts.items():
            self.logger.info(f"Initializing position client for {name}")
            
            # Create position client with account-specific settings
            client = PositionWebsocketClient(
                api_key=account['api_key'],
                api_secret=account['api_secret'],
                logger=self.logger
            )
            
            # Override WebSocket URL and other settings
            client.base_url = account['api_base_url']
            client.ws_url = account['ws_url']
            
            # Set environment variables for this client (needed for other utility functions)
            # Note: In the future, we could consider passing these directly to avoid env vars
            os.environ[f'POSITION_CLIENT_{name}_API_USERNAME'] = account['api_username']
            os.environ[f'POSITION_CLIENT_{name}_API_PASSWORD'] = account['api_password']
            os.environ[f'POSITION_CLIENT_{name}_API_CODE'] = account['api_code']
            os.environ[f'POSITION_CLIENT_{name}_API_BASE_URL'] = account['api_base_url']
            
            self.position_clients[name] = client
            
    def start_all_clients(self):
        """Start all position clients"""
        for name, client in self.position_clients.items():
            self.logger.info(f"Starting position client for {name}")
            client.start()
            
            # Give each client time to connect to prevent overloading
            time.sleep(1)
            
        # Allow clients some time to connect and establish sessions
        self.logger.info("Waiting for position clients to connect...")
        time.sleep(3)
        
        # Refresh positions for all clients
        for name, client in self.position_clients.items():
            if client.is_connected():
                client.refresh_positions()
                self.logger.info(f"Refreshed positions for {name}")
            else:
                self.logger.warning(f"Position client for {name} not connected")
            
    def stop_all_clients(self):
        """Stop all position clients"""
        for name, client in self.position_clients.items():
            self.logger.info(f"Stopping position client for {name}")
            client.stop()
            
    def get_position_client(self, account_name):
        """Get position client for a specific account"""
        if account_name in self.position_clients:
            return self.position_clients[account_name]
        return self.position_clients.get('default')
        
    def get_all_position_clients(self):
        """Get all position clients"""
        return self.position_clients
        
    def verify_repo_status(self, symbol, account_name):
        """
        Verify repo status for a specific account
        """
        position_client = self.get_position_client(account_name)
        if not position_client:
            self.logger.error(f"No position client for account: {account_name}")
            return False
            
        # Get the account config
        account = self.accounts.get(account_name)
        if not account:
            self.logger.error(f"No account configuration for: {account_name}")
            return False
            
        base_currency = symbol.split('/')[0]
        repo_symbol = f"{base_currency}/USDC110"
        
        # Check WebSocket tracking
        ws_repo_status = position_client.has_repo(symbol)
        
        try:
            # Setup for this account
            os.environ['API_USERNAME'] = account['api_username']
            os.environ['API_PASSWORD'] = account['api_password']
            os.environ['API_CODE'] = account['api_code']
            os.environ['API_BASE_URL'] = account['api_base_url']
            
            # Import here to avoid circular imports
            from src.trading_utils import get_jwt_token, get_repo_details
            
            jwt_token = get_jwt_token()
            if jwt_token:
                repo_details = get_repo_details(jwt_token, repo_symbol, self.logger)
                api_repo_status = repo_details is not None
                
                # If API check succeeds, trust it over WebSocket
                if ws_repo_status != api_repo_status:
                    self.logger.warning(
                        f"[{account_name}] Repo status mismatch for {symbol}: "
                        f"WebSocket={ws_repo_status}, API={api_repo_status}"
                    )
                    # Update WebSocket tracking to match API
                    position_client.set_repo_status(symbol, api_repo_status)
                    return api_repo_status
                    
        except Exception as e:
            # On API error, log but don't change behavior
            self.logger.debug(f"[{account_name}] Error checking repo status via API: {str(e)}")
        
        # If API check fails or isn't available, use WebSocket data
        return ws_repo_status
        
    def refresh_all_positions(self):
        """Force a refresh of positions for all clients"""
        for name, client in self.position_clients.items():
            try:
                client.refresh_positions()
                self.logger.debug(f"Refreshed positions for {name}")
            except Exception as e:
                self.logger.error(f"Error refreshing positions for {name}: {str(e)}")