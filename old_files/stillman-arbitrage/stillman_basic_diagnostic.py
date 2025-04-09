#!/usr/bin/env python3
import json
import time
import hmac
import hashlib
import logging
import sys
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stillman_diagnostic.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("stillman-diagnostic")

# Import configuration
from config import STILLMAN_CONFIG

def create_signature():
    """Create authentication signature for Stillman API"""
    timestamp = int(time.time())
    string_to_sign = f"GET/v1/stream{timestamp}"
    
    try:
        logger.debug(f"Public Key: {STILLMAN_CONFIG['publicKey']}")
        logger.debug(f"Private Key (first 5 chars): {STILLMAN_CONFIG['privateKey'][:5]}...")
        logger.debug(f"String to sign: {string_to_sign}")
        
        signature = hmac.new(
            STILLMAN_CONFIG['privateKey'].encode(),
            string_to_sign.encode(),
            hashlib.sha256
        ).hexdigest()
        
        logger.debug(f"Generated signature: {signature}")
        logger.debug(f"Timestamp: {timestamp}")
        return signature, timestamp
    except Exception as e:
        logger.error(f"Error creating signature: {e}")
        raise

def check_configuration():
    """Verify configuration values"""
    logger.info("Checking configuration...")
    
    # Check public key
    if len(STILLMAN_CONFIG['publicKey']) != 25:  # Typically 25 characters
        logger.warning(f"Public key length ({len(STILLMAN_CONFIG['publicKey'])}) might be incorrect")
    
    # Check private key
    if len(STILLMAN_CONFIG['privateKey']) != 48:  # Typically 48 characters
        logger.warning(f"Private key length ({len(STILLMAN_CONFIG['privateKey'])}) might be incorrect")
    
    # Check URL
    expected_urls = [
        "wss://sandbox-api.stillmandigital.com/v1/stream",
        "wss://api.stillmandigital.com/v1/stream",
        "wss://api.stillmanglobal.com/v1/stream"
    ]
    
    if STILLMAN_CONFIG['wsUrl'] not in expected_urls:
        logger.warning(f"URL '{STILLMAN_CONFIG['wsUrl']}' might be incorrect")
    
    # Log all config values
    for key, value in STILLMAN_CONFIG.items():
        if key in ['publicKey', 'privateKey']:
            logger.info(f"{key}: {value[:5]}...{value[-5:]}")
        else:
            logger.info(f"{key}: {value}")
            
    # Log alternative signature formats to try
    try_alternative_signatures(STILLMAN_CONFIG['privateKey'])

def try_alternative_signatures(private_key):
    """Try different signature calculation methods"""
    timestamp = int(time.time())
    
    # Method 1: Original (GET/v1/stream + timestamp)
    string1 = f"GET/v1/stream{timestamp}"
    sig1 = hmac.new(private_key.encode(), string1.encode(), hashlib.sha256).hexdigest()
    
    # Method 2: With space (GET /v1/stream + timestamp)
    string2 = f"GET /v1/stream{timestamp}"
    sig2 = hmac.new(private_key.encode(), string2.encode(), hashlib.sha256).hexdigest()
    
    # Method 3: Just the path (/v1/stream + timestamp)
    string3 = f"/v1/stream{timestamp}"
    sig3 = hmac.new(private_key.encode(), string3.encode(), hashlib.sha256).hexdigest()
    
    # Method 4: Full URL 
    string4 = f"{STILLMAN_CONFIG['wsUrl']}{timestamp}"
    sig4 = hmac.new(private_key.encode(), string4.encode(), hashlib.sha256).hexdigest()
    
    # Method 5: Just timestamp
    string5 = f"{timestamp}"
    sig5 = hmac.new(private_key.encode(), string5.encode(), hashlib.sha256).hexdigest()
    
    logger.info("Alternative signature calculations:")
    logger.info(f"1. String: '{string1}' → Signature: {sig1}")
    logger.info(f"2. String: '{string2}' → Signature: {sig2}")
    logger.info(f"3. String: '{string3}' → Signature: {sig3}")
    logger.info(f"4. String: '{string4}' → Signature: {sig4}")
    logger.info(f"5. String: '{string5}' → Signature: {sig5}")

def main():
    """Main diagnostic function"""
    try:
        logger.info("=== STILLMAN API DIAGNOSTICS ===")
        logger.info(f"Current time: {datetime.now()}")
        logger.info(f"Unix timestamp: {int(time.time())}")
        
        # Check configuration
        check_configuration()
        
        # Generate instructions for manual testing
        generate_manual_testing_instructions()
        
        logger.info("Basic diagnostics complete")
    except Exception as e:
        logger.error(f"Diagnostic error: {e}")
        import traceback
        logger.error(traceback.format_exc())

def generate_manual_testing_instructions():
    """Generate instructions for manual testing with curl"""
    signature, timestamp = create_signature()
    
    logger.info("\n=== MANUAL TESTING INSTRUCTIONS ===")
    logger.info("You can test the API connection manually using curl:")
    
    curl_cmd = f"""
curl -v -X GET "https://sandbox-api.stillmandigital.com/v1/ping" \\
  -H "api-key: {STILLMAN_CONFIG['publicKey']}" \\
  -H "api-signature: {signature}" \\
  -H "api-timestamp: {timestamp}"
"""
    
    logger.info(curl_cmd)
    logger.info("\nCopy and run this command to test API connectivity.")
    logger.info("The -v flag will show detailed information including response headers and body.")
    logger.info("=== END MANUAL TESTING INSTRUCTIONS ===")

if __name__ == "__main__":
    main()