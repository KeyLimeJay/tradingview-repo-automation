#config.py
"""
Configuration settings for the XRP Matcher application.
Loads settings from environment variables.
"""

import os
import logging
from dotenv import load_dotenv  # Changed from 'import dotenv'

# Load environment variables from .env file
load_dotenv()

# Bitstamp API Configuration
BITSTAMP_API_KEY = os.getenv("BITSTAMP_API_KEY")
BITSTAMP_API_SECRET = os.getenv("BITSTAMP_API_SECRET")
BITSTAMP_SUBACCOUNT_ID = os.getenv('BITSTAMP_SUBACCOUNT_ID')
BITSTAMP_WS_URL = os.getenv("BITSTAMP_WS_URL")

# Bosonic API Configuration
BOSONIC_API_USERNAME = os.getenv("BOSONIC_API_USERNAME")
BOSONIC_API_PASSWORD = os.getenv("BOSONIC_API_PASSWORD")
BOSONIC_API_CODE = os.getenv("BOSONIC_API_CODE")
BOSONIC_API_BASE_URL = os.getenv("BOSONIC_API_BASE_URL")
BOSONIC_WS_URL = os.getenv("BOSONIC_WS_URL")

# Output Settings
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "data")
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "xrp_matched_trades.csv")

# Logging Settings
LOG_LEVEL_STR = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", "logs/xrp_matcher.log")

# Convert string log level to logging constant
LOG_LEVEL = getattr(logging, LOG_LEVEL_STR)

# Ensure required directories exist
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)