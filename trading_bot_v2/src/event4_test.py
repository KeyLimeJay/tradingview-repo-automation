#!/usr/bin/env python3

import os
import json
import time
import sys
from datetime import datetime

# Set environment variables for debug mode
os.environ['TRADING_BOT_DEBUG'] = "1"
os.environ['TRADING_BOT_API_TRACE'] = "1"
os.environ['TRADING_BOT_LOG_LEVEL'] = "DEBUG"

# Import modules after setting environment variables
sys.path.append('/opt/otcxn/tradingview-repo-automation/trading_bot_v2')
from src.trading_bot import TradingBot

# Create a test bot instance
bot = TradingBot()

# Simulate an Event 4 situation - ETH/USDT with a Sell signal
test_data = {
    "symbol": "ETH/USDT",
    "message": "Trend Sell!",
    "price": "2500",
    "timeFrame": "1h"
}

# Execute the webhook handler
print(f"[TEST] Starting Event 4 test at {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")

# We'll use the regular webhook handler but with our test data
try:
    from flask import Request
    
    # Create a request-like object with our test data
    class MockRequest:
        @property
        def json(self):
            return test_data
    
    # Save the original request object
    original_request = getattr(sys.modules.get('flask'), 'request', None)
    
    # Replace with our mock
    sys.modules['flask'].request = MockRequest()
    
    # Execute the webhook handler
    response = bot.webhook()
    
    # Restore the original
    if original_request:
        sys.modules['flask'].request = original_request
    
    print(f"[TEST] Response: {json.dumps(response[0].json, indent=2)}")
except Exception as e:
    print(f"[TEST] Error: {str(e)}")

print(f"[TEST] Test completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")
