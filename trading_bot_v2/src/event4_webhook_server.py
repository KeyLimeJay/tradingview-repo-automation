#!/usr/bin/env python3

import os
import sys
import time
import json
import logging
import datetime
from flask import Flask, request, jsonify
from pathlib import Path

# Add the trading_bot_v2 directory to the path
sys.path.append('/opt/otcxn/tradingview-repo-automation/trading_bot_v2')

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("event4_webhook")

# Import necessary functions
try:
    from src.config_manager import ConfigurationManager
    from src.trading_utils import place_order, place_repo_order, get_jwt_token
    logger.info("Successfully imported required modules")
except Exception as e:
    logger.error(f"Failed to import required modules: {str(e)}")
    sys.exit(1)

# Create Flask app
app = Flask(__name__)

def execute_event4(account_name, symbol, price, close_position_quantity, repo_quantity, short_position_quantity):
    """
    Execute the Event 4 sequence: sell, repo, sell
    
    Args:
        account_name: Account name in configuration
        symbol: Trading pair symbol (e.g., "ETH/USDT")
        price: Base price for orders
        close_position_quantity: Quantity for first sell (close position)
        repo_quantity: Quantity for repo
        short_position_quantity: Quantity for second sell (open short position)
    """
    # Create a unique request ID for logging
    request_id = f"REQ-{int(time.time())}"
    
    # Get configuration
    config_manager = ConfigurationManager()
    logger.info(f"[{request_id}] Loaded configuration for account: {account_name}")
    
    # Execute Step 1: First sell to close position
    logger.info(f"[{request_id}] EVENT 4 - STEP 1: First sell to close position with quantity {close_position_quantity}")
    first_sell_response = place_order(
        symbol=symbol,
        side="ASK",
        price=price,
        quantity=close_position_quantity,
        config_manager=config_manager,
        account_name=account_name,
        tif="GTC"
    )
    
    if first_sell_response:
        logger.info(f"[{request_id}] EVENT 4 - STEP 1: First sell succeeded")
        logger.info(f"[{request_id}] Response: {json.dumps(first_sell_response, indent=2, default=str)}")
    else:
        logger.error(f"[{request_id}] EVENT 4 - STEP 1: First sell failed")
        return {"success": False, "error": "First sell failed"}
    
    # Wait for the first order to be processed
    logger.info(f"[{request_id}] Waiting 5 seconds for order processing...")
    time.sleep(5)
    
    # Execute Step 2: Open repo
    logger.info(f"[{request_id}] EVENT 4 - STEP 2: Open repo with quantity {repo_quantity}")
    
    # Get interest rate from configuration
    interest_rate = float(config_manager.get_trading_setting(
        account_name, 'repo_interest_rate', 10.0))
    
    repo_response = place_repo_order(
        symbol=symbol,
        quantity=repo_quantity,
        interest_rate=interest_rate,
        config_manager=config_manager,
        account_name=account_name
    )
    
    if repo_response:
        logger.info(f"[{request_id}] EVENT 4 - STEP 2: Open repo succeeded")
        logger.info(f"[{request_id}] Response: {json.dumps(repo_response, indent=2, default=str)}")
    else:
        logger.error(f"[{request_id}] EVENT 4 - STEP 2: Open repo failed")
        return {"success": False, "error": "Open repo failed"}
    
    # Wait for the repo to be processed
    logger.info(f"[{request_id}] Waiting 10 seconds for repo processing...")
    time.sleep(10)
    
    # Execute Step 3: Second sell to open short position
    logger.info(f"[{request_id}] EVENT 4 - STEP 3: Second sell to open short with quantity {short_position_quantity}")
    second_sell_response = place_order(
        symbol=symbol,
        side="ASK",
        price=price,
        quantity=short_position_quantity,
        config_manager=config_manager,
        account_name=account_name,
        tif="GTC"
    )
    
    if second_sell_response:
        logger.info(f"[{request_id}] EVENT 4 - STEP 3: Second sell succeeded")
        logger.info(f"[{request_id}] Response: {json.dumps(second_sell_response, indent=2, default=str)}")
    else:
        logger.error(f"[{request_id}] EVENT 4 - STEP 3: Second sell failed")
        return {"success": False, "error": "Second sell failed"}
    
    logger.info(f"[{request_id}] EVENT 4 sequence completed successfully!")
    return {
        "success": True,
        "message": "Event 4 completed",
        "steps": [
            {"step": "first_sell", "response": first_sell_response},
            {"step": "open_repo", "response": repo_response},
            {"step": "second_sell", "response": second_sell_response}
        ]
    }

@app.route('/webhook', methods=['POST'])
def webhook():
    """Handle webhook requests."""
    try:
        # Get data from request
        data = request.json
        
        # Log the incoming webhook
        logger.info(f"Received webhook: {json.dumps(data, indent=2)}")
        
        # Extract data from webhook
        symbol = data.get('symbol')
        message = data.get('message')
        price = data.get('price')
        timeframe = data.get('timeFrame')
        
        # Validate required fields
        if not all([symbol, message, price, timeframe]):
            logger.error(f"Missing required fields in webhook: {data}")
            return jsonify({"success": False, "error": "Missing required fields"}), 400
        
        # Check if this is a sell signal
        if message != "Trend Sell!":
            logger.info(f"Received {message} signal, Event 4 only triggers on Trend Sell!")
            return jsonify({"success": True, "message": "Signal received but not a sell signal"}), 200
        
        # Find accounts that match the timeframe
        config = ConfigurationManager()
        account_name = None
        
        # Get all accounts from config
        for acc in config.get_enabled_accounts():
            acc_name = acc.get('name')
            timeframes = config.get_account_setting(acc_name, 'timeframes', [])
            trading_pairs = config.get_account_setting(acc_name, 'trading_pairs', [])
            
            # Check if this account handles this timeframe and trading pair
            if timeframe in timeframes and symbol in trading_pairs:
                account_name = acc_name
                break
        
        if not account_name:
            logger.error(f"No account configured for timeframe {timeframe} and symbol {symbol}")
            return jsonify({"success": False, "error": "No matching account configuration"}), 400
        
        # Get quantities from configuration
        base_currency = symbol.split('/')[0]  # 'ETH' from 'ETH/USDT'
        
        # Get the quantities from the currency configuration
        close_position_quantity = float(config.get_currency_setting(
            account_name, base_currency, 'max_quantity', 0.1))
        repo_quantity = float(config.get_currency_setting(
            account_name, base_currency, 'repo_qty', 0.1))
        short_position_quantity = float(config.get_currency_setting(
            account_name, base_currency, 'max_quantity', 0.1))
        
        logger.info(f"Executing Event 4 for account {account_name}:")
        logger.info(f"Symbol: {symbol}")
        logger.info(f"Price: {price}")
        logger.info(f"Close Position Quantity: {close_position_quantity}")
        logger.info(f"Repo Quantity: {repo_quantity}")
        logger.info(f"Short Position Quantity: {short_position_quantity}")
        
        # Execute Event 4 sequence
        result = execute_event4(
            account_name,
            symbol,
            price,
            close_position_quantity,
            repo_quantity,
            short_position_quantity
        )
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({
        "status": "ok",
        "timestamp": datetime.datetime.now().isoformat()
    }), 200

if __name__ == "__main__":
    # Get port from configuration
    config = ConfigurationManager()
    port = int(config.get_global_setting('port', 6100))
    host = config.get_global_setting('host', '0.0.0.0')
    
    logger.info(f"Starting webhook server on {host}:{port}")
    logger.info("Ready to receive Event 4 signals!")
    
    app.run(host=host, port=port, debug=False)