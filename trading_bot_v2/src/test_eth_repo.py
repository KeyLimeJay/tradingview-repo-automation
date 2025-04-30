#!/usr/bin/env python3
import os
import logging
import json
import time
import hmac
import hashlib
import base64
import requests

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('eth_repo_test')

# Load credentials from file
def load_credentials(file_path):
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading credentials: {str(e)}")
        return None

def generate_repo_clordid():
    """Generate a unique client order ID for repo orders"""
    import random
    import string
    now = time.strftime("%Y%m%d%H%M%S")
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))
    return f"WEB:{random_suffix}-{now}"

def place_repo_order(credentials, symbol, quantity, interest_rate):
    """Place a repo order using credentials"""
    logger.info(f"Placing repo order for {symbol}, quantity={quantity}, interest_rate={interest_rate}%")
    
    # Set up URL
    url = f"{credentials['api_url']}/rest/orders"
    endpoint = "/rest/orders"
    
    # Generate a unique repo client order ID
    clordid = generate_repo_clordid()
    
    # Create repo order data
    order_data = {
        "side": "BID",  # BID for borrowing
        "price": float(interest_rate),
        "custodianId": credentials['custodian_id'],
        "symbol": symbol,
        "currency": symbol.split('/')[0],
        "currency2": "USDC110",
        "orderQty": float(quantity),
        "clOrdId": clordid,
        "orderType": "LIMIT",
        "tif": "GTC",
        "dark": False,
        "isAvgPrice": False,
        "venue": "LIT"
    }
    
    # Convert order data to JSON
    body = json.dumps(order_data)
    
    # Create signature using body hash method
    body_hash = base64.b64encode(hashlib.md5(body.encode()).digest()).decode()
    string_to_sign = f"POST\n{endpoint}\n{body_hash}"
    signature = base64.b64encode(
        hmac.new(credentials['api_secret'].encode(), string_to_sign.encode(), hashlib.sha256).digest()
    ).decode()
    
    # Set up headers
    headers = {
        'api-key': credentials['api_key'],
        'api-sign': signature,
        'Content-Type': 'application/json'
    }
    
    logger.info(f"Making API request to {url}")
    logger.debug(f"Headers: {headers}")
    logger.debug(f"Body: {body}")
    
    try:
        # Make the API request
        response = requests.post(url, headers=headers, data=body, timeout=30)
        
        if response.ok:
            logger.info(f"Repo order placed successfully: {response.status_code}")
            if response.text:
                return response.json()
            return {"status": "success"}
        else:
            logger.error(f"Error placing repo order: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Exception placing repo order: {str(e)}")
        return None

def get_jwt_token(credentials):
    """Get JWT token for authentication"""
    logger.info("Getting JWT token...")
    
    base_url = credentials['api_base_url']
    if not base_url.endswith('/'):
        base_url += '/'
    
    url = f"{base_url}sso/api/login"
    
    payload = {
        "username": credentials['api_username'],
        "password": credentials['api_password'],
        "code": credentials['api_code'],
        "redirectTo": f"{base_url}/trader",
        "email": credentials['api_username']
    }
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'Origin': base_url,
        'Referer': f'{base_url}/login?noredir=1'
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        
        if response.status_code == 200:
            token = response.headers.get('Authorization', response.headers.get('authorization'))
            if token and token.startswith('Bearer '):
                token = token.replace('Bearer ', '')
            
            logger.info("Authentication successful")
            return token
        else:
            logger.error(f"Authentication failed: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error during authentication: {str(e)}")
        return None

def check_existing_repo(credentials, symbol):
    """Check if a repo already exists"""
    logger.info(f"Checking if repo already exists for {symbol}")
    
    # Get JWT token
    jwt_token = get_jwt_token(credentials)
    if not jwt_token:
        logger.error("Failed to get JWT token")
        return None
    
    # Set headers
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }
    
    # Form URL
    base_url = credentials['api_base_url']
    if not base_url.endswith('/'):
        base_url += '/'
    
    url = f"{base_url}rest/repocontract?sortBy=id&sortDirection=DESC&status=OPEN&repoSymbol={symbol}"
    
    # Create payload
    payload = {
        "userId": credentials['api_username'],
        "contractType": "BORROW",
        "eventId": f"event{int(time.time())}",
        "repoSymbol": symbol
    }
    
    try:
        # Make the request
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if not response.ok:
            logger.error(f"Failed to check repo status: {response.status_code} - {response.text}")
            return None
        
        repo_data = response.json()
        
        if not repo_data.get("content") or len(repo_data["content"]) == 0:
            logger.info(f"No open repo found for {symbol}")
            return None
        
        # Get the first open repo
        repo_contract = repo_data["content"][0]
        repo_id = repo_contract.get("id")
        
        if repo_id:
            logger.info(f"Found existing repo with ID: {repo_id}")
            return {"id": repo_id}
        else:
            logger.warning("No repo ID found in response")
            return None
    except Exception as e:
        logger.error(f"Error checking repo status: {str(e)}")
        return None

def main():
    # Path to credentials file
    creds_file = "/opt/otcxn/tradingview-repo-automation/trading_bot_v2/config/credentials/tm6.json"
    
    # Load credentials
    credentials = load_credentials(creds_file)
    if not credentials:
        logger.error("Failed to load credentials")
        return
    
    # Define repo parameters
    symbol = "ETH/USDC110"
    quantity = 0.1
    interest_rate = 10.0
    
    # Check if repo already exists
    existing_repo = check_existing_repo(credentials, symbol)
    if existing_repo:
        logger.warning(f"Repo already exists for {symbol}. Skipping repo creation.")
        return
    
    # Place repo order
    result = place_repo_order(credentials, symbol, quantity, interest_rate)
    
    if result:
        logger.info("Repo created successfully!")
        logger.info(f"Response: {json.dumps(result, indent=2)}")
    else:
        logger.error("Failed to create repo")

if __name__ == "__main__":
    main()