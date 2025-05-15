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
logger = logging.getLogger("event_webhook")

# Import necessary functions
try:
    from src.config_manager import ConfigurationManager
    from src.trading_utils import place_order, place_repo_order, close_repo, get_jwt_token
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

def execute_event1(account_name, symbol, price, close_short_quantity, close_repo_flag, long_position_quantity):
    """
    Execute the Event 1 sequence: buy, close repo, buy
    
    Args:
        account_name: Account name in configuration
        symbol: Trading pair symbol (e.g., "ETH/USDT")
        price: Base price for orders
        close_short_quantity: Quantity for first buy (close short position)
        close_repo_flag: Whether to close the repo
        long_position_quantity: Quantity for second buy (open long position)
    """
    # Create a unique request ID for logging
    request_id = f"REQ-{int(time.time())}"
    
    # Get configuration
    config_manager = ConfigurationManager()
    logger.info(f"[{request_id}] Loaded configuration for account: {account_name}")
    
    # Execute Step 1: First buy to close short position
    logger.info(f"[{request_id}] EVENT 1 - STEP 1: First buy to close short position with quantity {close_short_quantity}")
    first_buy_response = place_order(
        symbol=symbol,
        side="BID",
        price=price,
        quantity=close_short_quantity,
        config_manager=config_manager,
        account_name=account_name,
        tif="GTC"
    )
    
    if first_buy_response:
        logger.info(f"[{request_id}] EVENT 1 - STEP 1: First buy succeeded")
        logger.info(f"[{request_id}] Response: {json.dumps(first_buy_response, indent=2, default=str)}")
    else:
        logger.error(f"[{request_id}] EVENT 1 - STEP 1: First buy failed")
        return {"success": False, "error": "First buy failed"}
    
    # Wait for the first order to be processed
    logger.info(f"[{request_id}] Waiting 5 seconds for order processing...")
    time.sleep(5)
    
    # Step 2: Close repo
    if close_repo_flag:
        logger.info(f"[{request_id}] EVENT 1 - STEP 2: Closing repo for {symbol}")
        
        # Get JWT token for repo operations
        jwt_token = get_jwt_token(account_name=account_name, config_manager=config_manager)
        
        if not jwt_token:
            logger.error(f"[{request_id}] EVENT 1 - STEP 2: Failed to get JWT token for closing repo")
            return {"success": False, "error": "Failed to get JWT token for closing repo"}
        
        close_repo_response = close_repo(
            jwt_token=jwt_token,
            symbol=symbol,
            logger=logger,
            account_name=account_name,
            config_manager=config_manager
        )
        
        if close_repo_response:
            logger.info(f"[{request_id}] EVENT 1 - STEP 2: Close repo succeeded")
        else:
            logger.error(f"[{request_id}] EVENT 1 - STEP 2: Close repo failed or no repo found")
            # Continue even if repo close fails - it might not exist
    else:
        logger.info(f"[{request_id}] EVENT 1 - STEP 2: Skipping repo close as requested")
    
    # Wait for the repo closing to be processed
    logger.info(f"[{request_id}] Waiting 10 seconds for repo closing to process...")
    time.sleep(10)
    
    # Execute Step 3: Second buy to open long position
    logger.info(f"[{request_id}] EVENT 1 - STEP 3: Second buy to open long with quantity {long_position_quantity}")
    second_buy_response = place_order(
        symbol=symbol,
        side="BID",
        price=price,
        quantity=long_position_quantity,
        config_manager=config_manager,
        account_name=account_name,
        tif="GTC"
    )
    
    if second_buy_response:
        logger.info(f"[{request_id}] EVENT 1 - STEP 3: Second buy succeeded")
        logger.info(f"[{request_id}] Response: {json.dumps(second_buy_response, indent=2, default=str)}")
    else:
        logger.error(f"[{request_id}] EVENT 1 - STEP 3: Second buy failed")
        return {"success": False, "error": "Second buy failed"}
    
    logger.info(f"[{request_id}] EVENT 1 sequence completed successfully!")
    return {
        "success": True,
        "message": "Event 1 completed",
        "steps": [
            {"step": "first_buy", "response": first_buy_response},
            {"step": "close_repo", "response": True},
            {"step": "second_buy", "response": second_buy_response}
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
        quantity = float(config.get_currency_setting(
            account_name, base_currency, 'max_quantity', 0.1))
        repo_qty = float(config.get_currency_setting(
            account_name, base_currency, 'repo_qty', 0.1))
        
        # Check message type and execute appropriate event
        if message == "Trend Sell!":
            logger.info(f"Executing Event 4 (Sell-Repo-Sell) for account {account_name}:")
            logger.info(f"Symbol: {symbol}")
            logger.info(f"Price: {price}")
            
            # Execute Event 4 sequence
            result = execute_event4(
                account_name,
                symbol,
                price,
                quantity,     # Close position quantity
                repo_qty,     # Repo quantity
                quantity      # Short position quantity
            )
            
            return jsonify(result), 200
            
        elif message == "Trend Buy!":
            logger.info(f"Executing Event 1 (Buy-CloseRepo-Buy) for account {account_name}:")
            logger.info(f"Symbol: {symbol}")
            logger.info(f"Price: {price}")
            
            # Execute Event 1 sequence
            result = execute_event1(
                account_name,
                symbol,
                price,
                quantity,     # Close short position quantity
                True,         # Always try to close repo
                quantity      # Long position quantity
            )
            
            return jsonify(result), 200
            
        else:
            logger.warning(f"Unrecognized message type: {message}")
            return jsonify({
                "success": False, 
                "error": f"Unrecognized message type: {message}"
            }), 400
        
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
    logger.info("Ready to receive Event 1 (Buy-CloseRepo-Buy) and Event 4 (Sell-Repo-Sell) signals!")
    
    app.run(host=host, port=port, debug=False)