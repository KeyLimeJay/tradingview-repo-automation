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

def check_environment():
    """Check that required environment variables are set"""
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

def main():
    """Main entry point for the application"""
    logger = setup_logging()
    
    try:
        logger.info("Starting TradingBot application")
        
        # Check required environment variables
        missing_vars = check_environment()
        if missing_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
            logger.error("Please check your .env file and restart the application")
            sys.exit(1)
        
        # Show configuration details
        logger.info(f"API URL: {os.getenv('API_URL')}")
        logger.info(f"Base URL: {os.getenv('API_BASE_URL')}")
        logger.info(f"WebSocket URL: {os.getenv('WS_URL')}")
        logger.info(f"Trading Pairs: {os.getenv('TRADING_PAIRS')}")
        logger.info(f"Custodian ID: {os.getenv('CUSTODIAN_ID')}")
        
        # Create and run the trading bot
        bot = create_app()
        logger.info("Trading bot created successfully, starting now...")
        bot.run()
        
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()