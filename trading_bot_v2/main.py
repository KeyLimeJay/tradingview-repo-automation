#!/usr/bin/env python3
import os
import logging
import logging.handlers
import sys
import argparse
from pathlib import Path

def setup_logging():
    """Set up logging configuration"""
    if not os.path.exists('logs'):
        os.makedirs('logs')

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # File handler
    file_handler = logging.handlers.RotatingFileHandler(
        'logs/trading_bot.log',
        maxBytes=10000000,  # Default, will be overridden by config
        backupCount=5  # Default, will be overridden by config
    )
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Set up root logger
    root_logger = logging.getLogger()
    root_logger.setLevel('INFO')  # Default, will be overridden by config
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Return root logger
    return root_logger

def main():
    """Main entry point for the application"""
    # Set up command line arguments
    parser = argparse.ArgumentParser(description='Multi-Account Trading Bot')
    parser.add_argument(
        '--config', 
        type=str, 
        default=os.path.join('config', 'config.json'),
        help='Path to config file'
    )
    args = parser.parse_args()
    
    # Initialize logger first for early logging
    logger = setup_logging()
    
    try:
        # Validate that config file exists
        config_path = Path(args.config)
        if not config_path.exists():
            logger.error(f"Configuration file not found: {config_path}")
            sys.exit(1)
            
        logger.info(f"Starting Multi-Account TradingBot with config: {config_path}")
        
        # Import after logging setup to catch any import errors
        from src.trading_bot import create_app
        from src.config_manager import ConfigurationManager
        
        # Load and validate configuration
        config_manager = ConfigurationManager(config_path)
        if not config_manager.validate_config():
            logger.error("Configuration validation failed")
            sys.exit(1)
            
        # Create and run the trading bot
        bot = create_app(config_path)
        logger.info("Multi-account trading bot created successfully, starting now...")
        bot.run()
        
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()