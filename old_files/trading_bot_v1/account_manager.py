#account_manager.py
#!/usr/bin/env python3
import os
import logging
from dotenv import load_dotenv
from position_websocket import PositionWebsocketClient

class AccountConfig:
    """Configuration for a trading account"""
    def __init__(self, name, config_dict):
        self.name = name
        self.api_key = config_dict.get('api_key')
        self.api_secret = config_dict.get('api_secret')
        self.api_username = config_dict.get('api_username')
        self.api_password = config_dict.get('api_password')
        self.api_code = config_dict.get('api_code')
        self.api_url = config_dict.get('api_url')
        self.api_base_url = config_dict.get('api_base_url')
        self.ws_url = config_dict.get('ws_url')
        self.custodian_id = config_dict.get('custodian_id')
        self.trading_pairs = config_dict.get('trading_pairs', '').split(',')
        self.timeframes = config_dict.get('timeframes', '').split(',')
        
    def __str__(self):
        return f"Account: {self.name}, Timeframes: {','.join(self.timeframes)}, Pairs: {','.join(self.trading_pairs)}"

class AccountManager:
    """Manages multiple trading accounts"""
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger('account_manager')
        self.accounts = {}  # name -> AccountConfig
        self.position_clients = {}  # name -> PositionWebsocketClient
        self.timeframe_routing = {}  # timeframe -> account_name
        
    def load_accounts(self):
        """Load account configurations from environment variables"""
        # Default account (from main environment variables)
        default_account = {
            'name': 'default',
            'api_key': os.getenv('API_KEY'),
            'api_secret': os.getenv('API_SECRET'),
            'api_username': os.getenv('API_USERNAME'),
            'api_password': os.getenv('API_PASSWORD'),
            'api_code': os.getenv('API_CODE'),
            'api_url': os.getenv('API_URL'),
            'api_base_url': os.getenv('API_BASE_URL'),
            'ws_url': os.getenv('WS_URL'),
            'custodian_id': os.getenv('CUSTODIAN_ID'),
            'trading_pairs': os.getenv('TRADING_PAIRS', ''),
            'timeframes': os.getenv('DEFAULT_TIMEFRAMES', '1h')
        }
        
        self.add_account('default', default_account)
        
        # Load additional accounts from ACCOUNT_X environment variables
        for i in range(1, 10):  # Support up to 10 additional accounts
            account_prefix = f'ACCOUNT_{i}_'
            
            # Check if this account exists
            if not os.getenv(f'{account_prefix}NAME'):
                continue
                
            account_config = {
                'name': os.getenv(f'{account_prefix}NAME', f'account_{i}'),
                'api_key': os.getenv(f'{account_prefix}API_KEY'),
                'api_secret': os.getenv(f'{account_prefix}API_SECRET'),
                'api_username': os.getenv(f'{account_prefix}API_USERNAME'),
                'api_password': os.getenv(f'{account_prefix}API_PASSWORD'),
                'api_code': os.getenv(f'{account_prefix}API_CODE'),
                'api_url': os.getenv(f'{account_prefix}API_URL', os.getenv('API_URL')),
                'api_base_url': os.getenv(f'{account_prefix}API_BASE_URL'),
                'ws_url': os.getenv(f'{account_prefix}WS_URL'),
                'custodian_id': os.getenv(f'{account_prefix}CUSTODIAN_ID', os.getenv('CUSTODIAN_ID')),
                'trading_pairs': os.getenv(f'{account_prefix}TRADING_PAIRS', os.getenv('TRADING_PAIRS')),
                'timeframes': os.getenv(f'{account_prefix}TIMEFRAMES', '')
            }
            
            # Only add account if we have the key credentials
            if (account_config['api_key'] and account_config['api_secret'] and
                account_config['api_base_url'] and account_config['ws_url']):
                self.add_account(account_config['name'], account_config)
                
        self.setup_timeframe_routing()
        return len(self.accounts)
        
    def add_account(self, name, config_dict):
        """Add an account configuration"""
        account = AccountConfig(name, config_dict)
        self.accounts[name] = account
        self.logger.info(f"Added account: {account}")
        return account
        
    def setup_timeframe_routing(self):
        """Set up routing from timeframes to accounts"""
        # Clear existing routing
        self.timeframe_routing = {}
        
        for name, account in self.accounts.items():
            for timeframe in account.timeframes:
                timeframe = timeframe.strip()
                if not timeframe:
                    continue
                    
                if timeframe in self.timeframe_routing:
                    self.logger.warning(f"Timeframe {timeframe} already routed to {self.timeframe_routing[timeframe]}. Overriding with {name}.")
                    
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
                api_key=account.api_key,
                api_secret=account.api_secret,
                logger=self.logger
            )
            
            # Override WebSocket URL and other settings
            client.base_url = account.api_base_url
            client.ws_url = account.ws_url
            
            # Set environment variables for this client (needed for other utility functions)
            os.environ[f'POSITION_CLIENT_{name}_API_USERNAME'] = account.api_username
            os.environ[f'POSITION_CLIENT_{name}_API_PASSWORD'] = account.api_password
            os.environ[f'POSITION_CLIENT_{name}_API_CODE'] = account.api_code
            os.environ[f'POSITION_CLIENT_{name}_API_BASE_URL'] = account.api_base_url
            
            self.position_clients[name] = client
            
    def start_all_clients(self):
        """Start all position clients"""
        for name, client in self.position_clients.items():
            self.logger.info(f"Starting position client for {name}")
            client.start()
            
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
