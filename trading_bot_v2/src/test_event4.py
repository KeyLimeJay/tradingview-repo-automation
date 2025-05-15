#!/usr/bin/env python3

import os
import json
import time
import sys
import logging
import traceback
from datetime import datetime

# Set environment variables for debug mode
os.environ['TRADING_BOT_DEBUG'] = "1"
os.environ['TRADING_BOT_API_TRACE'] = "1"
os.environ['TRADING_BOT_LOG_LEVEL'] = "DEBUG"
os.environ['ENABLE_API_DEBUG'] = "1"  # Ensure API debug wrapper is active

print(f"[TEST] Setting up test environment at {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("test_event4")

# Import modules after setting environment variables
sys.path.append('/opt/otcxn/tradingview-repo-automation/trading_bot_v2')

try:
    from src.trading_utils import place_order, place_repo_order, get_jwt_token
    print("[TEST] Successfully imported trading_utils functions")
    
    # Check for debugging wrapper
    if hasattr(place_order, '_is_api_debug_wrapped'):
        print("[TEST] place_order has debug wrapper flag - good!")
    else:
        print("[TEST] WARNING: place_order does not have debug wrapper flag!")
        
    if hasattr(place_order, '__wrapped__'):
        print("[TEST] place_order has __wrapped__ attribute - function is wrapped")
    else:
        print("[TEST] WARNING: place_order doesn't appear to be wrapped!")
        
except Exception as e:
    print(f"[TEST] Import error: {str(e)}")
    traceback.print_exc()
    sys.exit(1)

# Test data for a direct function call
account_name = "TM6"  # Using TM6 account from your config
symbol = "ETH/USDT"   # This trading pair is configured for TM6
side = "ASK"          # For Event 4, we'd use ASK (sell)
price = "2500"
quantity = 0.1        # Small test quantity

print(f"\n[TEST] Testing direct order placement function:")
print(f"[TEST] Account: {account_name}")
print(f"[TEST] Symbol: {symbol}")
print(f"[TEST] Side: {side}")
print(f"[TEST] Price: {price}")
print(f"[TEST] Quantity: {quantity}")

# Try to get a JWT token first to test authentication
try:
    print("\n[TEST] Testing JWT token acquisition...")
    # Import ConfigManager to get account credentials
    from src.config_manager import ConfigurationManager
    config_manager = ConfigurationManager()
    
    # Get JWT token
    jwt_token = get_jwt_token(account_name=account_name, config_manager=config_manager)
    if jwt_token:
        print("[TEST] ✅ Successfully obtained JWT token")
    else:
        print("[TEST] ❌ Failed to get JWT token")
except Exception as e:
    print(f"[TEST] Error getting JWT token: {str(e)}")
    traceback.print_exc()

# Test order placement directly
try:
    print(f"\n[TEST] Attempting direct order placement at {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")
    
    # Small test order - using a GTC order so it won't execute immediately
    order_response = place_order(
        symbol=symbol,
        side=side,
        price=price,
        quantity=quantity,
        config_manager=config_manager,  # Pass the config manager
        account_name=account_name,      # Use the TM6 account name
        tif="GTC"                       # Use GTC for testing
    )
    
    if order_response:
        print(f"[TEST] ✅ Order placement succeeded!")
        print(f"[TEST] Response: {json.dumps(order_response, indent=2, default=str)}")
    else:
        print(f"[TEST] ❌ Order placement failed or returned no response")
        
except Exception as e:
    print(f"[TEST] Error during direct order placement: {str(e)}")
    traceback.print_exc()

print(f"\n[TEST] Script completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")