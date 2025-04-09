#main.py
#!/usr/bin/env python3
import os
import logging
import logging.handlers
import sys
from dotenv import load_dotenv
from trading_bot import create_app

# Load environment variables from .env file
load_dotenv()

def setup_logging():
    """Set up logging configuration"""
    if not os.path.exists('logs'):
        os.makedirs('logs')

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # File handler
    file_handler = logging.handlers.RotatingFileHandler(
        'logs/trading_bot.log',
        maxBytes=int(os.getenv('MAX_LOG_BYTES', 10000000)),
        backupCount=int(os.getenv('LOG_BACKUP_COUNT', 5))
    )
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Set up root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(os.getenv('LOG_LEVEL', 'INFO'))
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Return root logger
    return root_logger

def check_primary_environment():
    """Check that required environment variables for primary account are set"""
    required_vars = [
        "API_KEY", "API_SECRET", "API_URL", 
        "API_USERNAME", "API_PASSWORD", "API_CODE",
        "TRADING_PAIRS", "CUSTODIAN_ID",
        "API_BASE_URL", "WS_URL"
    ]
    
    missing = []
    for var in required_vars:
        if not os.getenv(var):
            missing.append(var)
    
    return missing

def check_additional_accounts():
    """Check for additional account configurations"""
    accounts = []
    
    # Look for additional accounts (ACCOUNT_1, ACCOUNT_2, etc)
    for i in range(1, 10):  # Support up to 10 additional accounts
        account_prefix = f'ACCOUNT_{i}_'
        
        # Check if this account exists by checking for the API key
        if not os.getenv(f'{account_prefix}API_KEY'):
            continue
            
        # Check required variables for this account
        required_vars = [
            f"{account_prefix}API_SECRET",
            f"{account_prefix}API_USERNAME",
            f"{account_prefix}API_PASSWORD",
            f"{account_prefix}API_CODE",
            f"{account_prefix}API_BASE_URL",
            f"{account_prefix}WS_URL",
            f"{account_prefix}TIMEFRAMES"
        ]
        
        missing = []
        for var in required_vars:
            if not os.getenv(var):
                missing.append(var)
        
        account_info = {
            'number': i,
            'name': os.getenv(f'{account_prefix}NAME', f'Account {i}'),
            'timeframes': os.getenv(f'{account_prefix}TIMEFRAMES', '').split(','),
            'missing': missing
        }
        
        accounts.append(account_info)
    
    return accounts

def main():
    """Main entry point for the application"""
    logger = setup_logging()
    
    try:
        logger.info("Starting Multi-Account TradingBot application")
        
        # Check required environment variables for primary account
        missing_vars = check_primary_environment()
        if missing_vars:
            logger.error(f"Missing required environment variables for primary account: {', '.join(missing_vars)}")
            logger.error("Please check your .env file and restart the application")
            sys.exit(1)
        
        # Check additional accounts
        additional_accounts = check_additional_accounts()
        if additional_accounts:
            logger.info(f"Found {len(additional_accounts)} additional account configurations")
            
            # Check for any missing variables in additional accounts
            for account in additional_accounts:
                if account['missing']:
                    logger.warning(f"Account {account['name']} (#{account['number']}) is missing variables: {', '.join(account['missing'])}")
                else:
                    logger.info(f"Account {account['name']} configured for timeframes: {', '.join(account['timeframes'])}")
        else:
            logger.info("No additional accounts configured, using primary account only")
        
        # Set default timeframes for primary account if not specified
        if not os.getenv('DEFAULT_TIMEFRAMES'):
            default_timeframes = "1h"  # Default to 1h if not specified
            logger.info(f"No DEFAULT_TIMEFRAMES specified for primary account, using: {default_timeframes}")
            os.environ['DEFAULT_TIMEFRAMES'] = default_timeframes
        else:
            logger.info(f"Primary account configured for timeframes: {os.getenv('DEFAULT_TIMEFRAMES')}")
        
        # Create and run the trading bot
        bot = create_app()
        logger.info("Multi-account trading bot created successfully, starting now...")
        bot.run()
        
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()