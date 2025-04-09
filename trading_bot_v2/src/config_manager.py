#config_manager.py
#!/usr/bin/env python3
import json
import os
import logging
import datetime
from pathlib import Path

class ConfigurationManager:
    """
    Manages loading and accessing configuration settings from JSON files.
    Supports both global settings and account-specific configurations.
    """
    def __init__(self, config_path=None):
        self.logger = logging.getLogger("config_manager")
        self.config_path = config_path or os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 
            "config", "config.json"
        )
        self.config = self._load_config()
        self.credentials = self._load_credentials()
        
    def _load_config(self):
        """Load main configuration file"""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
                self.logger.info(f"Loaded configuration from {self.config_path}")
                return config
        except Exception as e:
            self.logger.error(f"Failed to load configuration: {str(e)}")
            raise
            
    def _load_credentials(self):
        """Load credentials for all enabled accounts"""
        credentials = {}
        config_dir = os.path.dirname(self.config_path)
        
        for account in self.get_enabled_accounts():
            name = account.get('name')
            cred_path = account.get('credentials_file')
            if not cred_path:
                self.logger.error(f"No credentials file specified for account {name}")
                continue
                
            # Handle relative paths
            if not os.path.isabs(cred_path):
                cred_path = os.path.join(config_dir, cred_path)
                
            try:
                with open(cred_path, 'r') as f:
                    account_creds = json.load(f)
                    credentials[name] = account_creds
                    self.logger.info(f"Loaded credentials for account {name}")
            except Exception as e:
                self.logger.error(f"Failed to load credentials for {name}: {str(e)}")
                
        return credentials
        
    def get_global_setting(self, key, default=None):
        """Get a global setting with fallback"""
        return self.config.get('global', {}).get(key, default)
        
    def get_account_setting(self, account_name, key, default=None):
        """Get a setting for a specific account"""
        for account in self.config.get('accounts', []):
            if account.get('name') == account_name:
                return account.get(key, default)
        return default
        
    def get_trading_setting(self, account_name, key, default=None):
        """Get a trading setting for a specific account with fallback"""
        account = self.get_account(account_name)
        if not account:
            return default
        return account.get('trading', {}).get(key, default)
        
    def get_currency_setting(self, account_name, currency, key, default=None):
        """Get a currency-specific setting for an account with fallback"""
        account = self.get_account(account_name)
        if not account:
            return default
        return account.get('currencies', {}).get(currency, {}).get(key, default)
        
    def get_account_credentials(self, account_name):
        """Get credentials for a specific account"""
        return self.credentials.get(account_name, {})
        
    def get_account(self, account_name):
        """Get full account configuration by name"""
        for account in self.config.get('accounts', []):
            if account.get('name') == account_name:
                return account
        return None
        
    def get_enabled_accounts(self):
        """Get list of enabled account configurations"""
        return [account for account in self.config.get('accounts', []) 
                if account.get('enabled', True)]
                
    def get_account_for_timeframe(self, timeframe):
        """Find the account responsible for a specific timeframe"""
        for account in self.get_enabled_accounts():
            if timeframe in account.get('timeframes', []):
                return account.get('name')
        # If no specific account found, try to use default
        for account in self.get_enabled_accounts():
            if account.get('name') == 'default':
                return 'default'
        # If no default account, use the first enabled account
        if self.get_enabled_accounts():
            return self.get_enabled_accounts()[0].get('name')
        return None
                
    def get_all_trading_pairs(self):
        """Get all unique trading pairs across all accounts"""
        pairs = set()
        for account in self.get_enabled_accounts():
            pairs.update(account.get('trading_pairs', []))
        return list(pairs)
        
    def save_config(self):
        """Save the current configuration back to file"""
        try:
            # Create a backup of the current config
            backup_path = f"{self.config_path}.bak.{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as src:
                    with open(backup_path, 'w') as dst:
                        dst.write(src.read())
                        
            # Write the updated config
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
                
            self.logger.info(f"Saved configuration to {self.config_path} (backup at {backup_path})")
            return True
        except Exception as e:
            self.logger.error(f"Failed to save configuration: {str(e)}")
            return False
            
    def validate_config(self):
        """Basic validation of configuration structure"""
        try:
            if not self.config.get('global'):
                self.logger.error("Missing 'global' section in configuration")
                return False
                
            if not self.config.get('accounts'):
                self.logger.error("Missing 'accounts' section in configuration")
                return False
                
            if not self.get_enabled_accounts():
                self.logger.error("No enabled accounts found in configuration")
                return False
                
            # Check that all enabled accounts have required fields
            for account in self.get_enabled_accounts():
                name = account.get('name')
                if not name:
                    self.logger.error("Account missing 'name' field")
                    return False
                    
                if not account.get('credentials_file'):
                    self.logger.error(f"Account {name} missing 'credentials_file'")
                    return False
                    
                if not account.get('timeframes'):
                    self.logger.error(f"Account {name} missing 'timeframes'")
                    return False
                    
                if not account.get('trading_pairs'):
                    self.logger.error(f"Account {name} missing 'trading_pairs'")
                    return False
                    
            return True
        except Exception as e:
            self.logger.error(f"Configuration validation error: {str(e)}")
            return False