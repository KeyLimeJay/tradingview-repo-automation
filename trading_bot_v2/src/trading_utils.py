#trading_utils.py
#!/usr/bin/env python3
import requests
import hmac
import hashlib
import base64
import json
import time
import uuid
import random
import string
import os
import logging
import datetime
from pathlib import Path

# Configure logger
logger = logging.getLogger('trading_utils')

class OrderPlacementError(Exception):
    """Exception raised when an order fails to be placed."""
    pass

def generate_clordid(prefix="ORD"):
    """Generate a unique client order ID for regular orders"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    random_suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"{prefix}_{timestamp}_{random_suffix}"

def generate_repo_clordid():
    """Generate a unique client order ID for repo orders"""
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y%m%d%H%M%S")
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))
    return f"WEB:{random_suffix}-{timestamp}"

def get_price_precision(symbol, config_manager=None, account_name=None):
    """
    Get price precision for a symbol from configuration
    
    Args:
        symbol: Trading pair symbol
        config_manager: Optional ConfigurationManager instance
        account_name: Optional account name to get settings for
        
    Returns:
        Price precision (number of decimal places)
    """
    try:
        base_currency = symbol.split('/')[0]
        
        # Try to get precision from config manager if available
        if config_manager and account_name:
            precision = config_manager.get_currency_setting(
                account_name, base_currency, 'price_decimals', None)
            if precision is not None:
                return int(precision)
        
        # Fall back to environment variables
        return int(os.getenv(f'{base_currency}_PRICE_DECIMALS', 2))
    except (ValueError, TypeError, IndexError):
        # Default precision if anything goes wrong
        return 2

def adjust_price(price, side, symbol, config_manager=None, account_name=None):
    """
    Adjust price based on side and configured adjustments
    
    Args:
        price: Base price
        side: Order side (BID or ASK)
        symbol: Trading pair symbol
        config_manager: Optional ConfigurationManager instance
        account_name: Optional account name to get settings for
        
    Returns:
        Adjusted price
    """
    try:
        base_price = float(str(price).replace(',', ''))
        if base_price <= 0:
            raise ValueError("Price must be positive")
        
        # Get adjustment factors from config or environment
        if config_manager and account_name:
            bid_adj = float(config_manager.get_trading_setting(
                account_name, 'bid_adjustment', 1.05))
            ask_adj = float(config_manager.get_trading_setting(
                account_name, 'ask_adjustment', 0.95))
        else:
            bid_adj = float(os.getenv('BID_ADJUSTMENT', 1.05))
            ask_adj = float(os.getenv('ASK_ADJUSTMENT', 0.95))
            
        if side == "BID":
            adjusted_price = base_price * bid_adj
        else:
            adjusted_price = base_price * ask_adj
            
        precision = get_price_precision(
            symbol, config_manager=config_manager, account_name=account_name)
        return round(adjusted_price, precision)
        
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid price {price}: {str(e)}")

def create_signature(api_secret, method, endpoint, body_hash=None, timestamp=None):
    """
    Create signature for API request 
    
    Args:
        api_secret: API secret key
        method: HTTP method (GET, POST, etc.)
        endpoint: API endpoint path
        body_hash: Optional hash of the request body (for body hash method)
        timestamp: Timestamp to use (for timestamp method)
        
    Returns:
        Base64 encoded signature
    """
    try:
        if body_hash:
            # If body_hash is provided, use body hash method
            string_to_sign = f"{method}\n{endpoint}\n{body_hash}"
        elif timestamp:
            # If timestamp is provided, use timestamp method
            string_to_sign = f"{method}\n{endpoint}\n{timestamp}\n"
        else:
            raise ValueError("Either body_hash or timestamp must be provided")
        
        # Create HMAC signature
        signature = base64.b64encode(
            hmac.new(
                api_secret.encode(),
                string_to_sign.encode(),
                hashlib.sha256
            ).digest()
        ).decode()
        
        logger.debug(f"Signature Generation: {method} {endpoint} -> {signature[:10]}...")
        return signature
        
    except Exception as e:
        logger.error(f"Error creating signature: {str(e)}")
        raise

def place_order(api_key=None, api_secret=None, symbol=None, side=None, price=None, 
                quantity=None, custodian_id=None, tif=None, max_retries=None,
                config_manager=None, account_name=None):
    """
    Place a trading order with robust retry logic
    
    Args:
        api_key: API key for authentication
        api_secret: API secret for signing requests
        symbol: Trading pair symbol (e.g., "BTC/USDC")
        side: Order side - "BID" for buy, "ASK" for sell
        price: Order price
        quantity: Order quantity
        custodian_id: Custodian ID
        tif: Time in force (IOC, GTC, etc.)
        max_retries: Maximum number of retry attempts
        config_manager: Optional ConfigurationManager instance
        account_name: Optional account name for account-specific settings
    """
    # Get credentials from config if provided
    if config_manager and account_name:
        credentials = config_manager.get_account_credentials(account_name)
        api_key = api_key or credentials.get('api_key')
        api_secret = api_secret or credentials.get('api_secret')
        custodian_id = custodian_id or credentials.get('custodian_id')
        api_url = credentials.get('api_url')
        
        # Get trading settings from config
        tif = tif or config_manager.get_trading_setting(account_name, 'default_tif', 'GTC')
        max_retries = max_retries or int(config_manager.get_trading_setting(
            account_name, 'max_retries', 3))
        retry_delay = int(config_manager.get_trading_setting(
            account_name, 'retry_delay', 1))
    else:
        # Use environment variables when parameters are not provided
        api_key = api_key or os.getenv('API_KEY')
        api_secret = api_secret or os.getenv('API_SECRET')
        custodian_id = custodian_id or os.getenv('CUSTODIAN_ID')
        api_url = os.getenv('API_URL')
        tif = tif or os.getenv('DEFAULT_TIF', 'GTC')
        max_retries = max_retries or int(os.getenv('MAX_RETRIES', 3))
        retry_delay = int(os.getenv('RETRY_DELAY', 1))
    
    # Set up URL
    url = f"{api_url}/rest/orders"
    endpoint = "/rest/orders"
    method = "POST"
    
    logger.info(f"Placing Order - Symbol: {symbol}, Side: {side}, Price: {price}, Quantity: {quantity}")
    
    try:
        # Adjust price according to our strategy
        adjusted_price = adjust_price(price, side, symbol, config_manager, account_name)
        
        # Generate a unique client order ID
        clordid = generate_clordid()
        
        # Create order data
        order_data = {
            "side": side,
            "price": adjusted_price,
            "custodianId": custodian_id,
            "symbol": symbol,
            "orderQty": quantity,
            "clOrdId": clordid,
            "orderType": "LIMIT",
            "tif": tif,
            "dark": False,
            "isAvgPrice": False,
            "venue": "LIT"
        }
        
        # Extract currencies from symbol
        if '/' in symbol:
            currencies = symbol.split('/')
            if len(currencies) == 2:
                order_data["currency"] = currencies[0]
                order_data["currency2"] = currencies[1]
        
        # Convert order data to JSON
        body = json.dumps(order_data)
        
        for attempt in range(max_retries):
            try:
                logger.debug(f"Order placement attempt {attempt + 1}/{max_retries}")
                
                # Create signature using body hash method
                body_hash = base64.b64encode(hashlib.md5(body.encode()).digest()).decode()
                string_to_sign = f"{method}\n{endpoint}\n{body_hash}"
                signature = base64.b64encode(
                    hmac.new(api_secret.encode(), string_to_sign.encode(), hashlib.sha256).digest()
                ).decode()
                
                # Set up headers
                headers = {
                    'api-key': api_key,
                    'api-sign': signature,
                    'Content-Type': 'application/json'
                }
                
                logger.debug(f"Order request: {url}")
                logger.debug(f"Order body: {body}")
                
                # Make the API request
                response = requests.post(url, headers=headers, data=body)
                
                if response.ok:
                    logger.info(f"Order placed successfully: Status {response.status_code}")
                    if response.text:
                        return response.json()
                    return {}
                
                # Get the list of retriable errors from config if possible
                retry_conditions = [
                    "No custodian isos",
                    "No liquidity",
                    "IOC expired",
                    "Insufficient funds"
                ]
                
                if any(error in response.text for error in retry_conditions) and attempt < max_retries - 1:
                    logger.warning(f"Retriable error detected: {response.text}")
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                
                # Non-retriable error
                error_msg = f"Order placement failed: Status {response.status_code}, Details: {response.text}"
                logger.error(error_msg)
                raise OrderPlacementError(error_msg)
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Network error on attempt {attempt + 1}: {str(e)}")
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                raise OrderPlacementError(f"Network error: {str(e)}")
                
        raise OrderPlacementError(f"Failed to place order after {max_retries} attempts")
    
    except Exception as e:
        logger.error(f"Error in order placement: {str(e)}")
        raise

def get_jwt_token(account_name=None, config_manager=None):
    """
    Get JWT token for API access
    
    Args:
        account_name: Optional account name to get credentials for
        config_manager: Optional ConfigurationManager instance
        
    Returns:
        JWT token if successful, None otherwise
    """
    # Get credentials from config if provided
    if config_manager and account_name:
        credentials = config_manager.get_account_credentials(account_name)
        user = credentials.get('api_username')
        password = credentials.get('api_password')
        code = credentials.get('api_code')
        base_url = credentials.get('api_base_url')
    else:
        user = os.getenv("API_USERNAME")
        password = os.getenv("API_PASSWORD")
        code = os.getenv("API_CODE")
        base_url = os.getenv("API_BASE_URL")
    
    if not base_url.endswith('/'):
        base_url += '/'
    
    method = "POST"
    endpoint = "sso/api/login"
    url = base_url + endpoint
    
    payload = {
        "code": code,
        "password": password,
        "redirectTo": base_url,
        "username": user
    }
    
    body = json.dumps(payload)
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*'
    }
    
    logger.debug(f"Getting JWT token for {user}")
    
    try:
        response = requests.post(url, headers=headers, data=body)
        
        if not response.ok:
            logger.error(f"Failed to get JWT token: {response.status_code} - {response.text}")
            return None
            
        token = response.headers.get("authorization")
        if not token:
            logger.error("No authorization token in response headers")
            return None
            
        logger.info("Successfully obtained JWT token")
        return token
        
    except Exception as e:
        logger.error(f"Error getting JWT token: {str(e)}")
        return None

def get_repo_details(jwt_token=None, symbol=None, logger=None, 
                     account_name=None, config_manager=None):
    """
    Get details of open repo contracts for a symbol
    
    Args:
        jwt_token: JWT token for authentication
        symbol: Repo symbol (e.g., "BTC/USDC110")
        logger: Optional logger
        account_name: Optional account name to get base_url from
        config_manager: Optional ConfigurationManager instance
        
    Returns:
        Repo details if found, None otherwise
    """
    log = logger or logging.getLogger('trading_utils')
    
    if log:
        log.info(f"Getting repo details for {symbol}")
    
    # Get base_url from config if provided
    if config_manager and account_name:
        credentials = config_manager.get_account_credentials(account_name)
        base_url = credentials.get('api_base_url')
        username = credentials.get('api_username')
    else:
        base_url = os.getenv("API_BASE_URL")
        username = os.getenv("API_USERNAME")
    
    # Get JWT token if not provided
    if not jwt_token:
        jwt_token = get_jwt_token(account_name, config_manager)
        if not jwt_token:
            log.error("Failed to get JWT token")
            return None
    
    # Set headers
    headers = {
        "Authorization": jwt_token,
        "Content-Type": "application/json",
        "User-Agent": "python-requests/2.28.1"
    }
    
    if not base_url.endswith('/'):
        base_url += '/'
    
    # Create payload for POST request
    payload = {
        "userId": username,
        "contractType": "BORROW",
        "eventId": "event" + str(int(time.time())),
        "repoSymbol": symbol
    }
    
    # URL for repo details
    url = f"{base_url}rest/repocontract?sortBy=id&sortDirection=DESC&status=OPEN&repoSymbol={symbol}"
    
    try:
        # Make the POST request to get repo details
        repo_response = requests.post(
            url=url,
            headers=headers,
            json=payload
        )
        
        if log:
            log.debug(f"Repo details response: {repo_response.status_code}")
        
        if not repo_response.ok:
            if log:
                log.error(f"Failed to get repo details: {repo_response.text}")
            return None
        
        repo_data = repo_response.json()
        
        if not repo_data.get("content") or len(repo_data["content"]) == 0:
            if log:
                log.warning(f"No open repo found for {symbol}")
            return None
        
        # Get the first open repo
        repo_contract = repo_data["content"][0]
        repo_id = repo_contract.get("id")
        event_id = repo_contract.get("eventId")
        
        if not repo_id:
            if log:
                log.error("No repo ID found in response")
            return None
        
        if log:
            log.debug(f"Found repo with ID: {repo_id}, Event ID: {event_id}")
            
        return {
            "id": repo_id,
            "eventId": event_id
        }
        
    except Exception as e:
        if log:
            log.error(f"Error retrieving repo details: {str(e)}")
        return None

def place_repo_order(jwt_token=None, symbol=None, quantity=None, interest_rate=None, 
                     custodian_id=None, side="BID", api_key=None, api_secret=None, 
                     logger=None, account_name=None, config_manager=None):
    """
    Place a repo order (borrow/lend) using API key authentication
    
    Args:
        jwt_token: JWT token (not used, kept for compatibility)
        symbol: Repo symbol (e.g., "BTC/USDC110")
        quantity: Amount to borrow
        interest_rate: Interest rate (e.g., 10%)
        custodian_id: Custodian ID
        side: Order side (default: "BID" for borrowing)
        api_key: API key for authentication
        api_secret: API secret for signing requests
        logger: Logger object for logging
        account_name: Optional account name to get settings from
        config_manager: Optional ConfigurationManager instance
        
    Returns:
        Response from the API
    """
    log = logger or logging.getLogger('trading_utils')
    
    if log:
        log.info(f"Placing repo order - Symbol: {symbol}, Quantity: {quantity}, Rate: {interest_rate}%")
    
    # Get credentials and settings from config if provided
    if config_manager and account_name:
        credentials = config_manager.get_account_credentials(account_name)
        api_key = api_key or credentials.get('api_key')
        api_secret = api_secret or credentials.get('api_secret')
        custodian_id = custodian_id or credentials.get('custodian_id')
        api_url = credentials.get('api_url')
        interest_rate = interest_rate or float(config_manager.get_trading_setting(
            account_name, 'repo_interest_rate', 10.0))
    else:
        # Use env vars if not provided
        api_key = api_key or os.getenv('API_KEY')
        api_secret = api_secret or os.getenv('API_SECRET')
        custodian_id = custodian_id or os.getenv('CUSTODIAN_ID')
        api_url = os.getenv('API_URL')
        interest_rate = interest_rate or float(os.getenv('REPO_INTEREST_RATE', 10.0))
    
    # Set up URL
    url = f"{api_url}/rest/orders"
    endpoint = "/rest/orders"
    
    # Validate input
    if not symbol or not '/' in symbol:
        raise ValueError(f"Invalid symbol format: {symbol}. Expected format: BASE/QUOTE110")
    
    # Extract currencies from symbol
    base_currency = symbol.split('/')[0]
    
    # Check if a repo already exists for this symbol
    jwt_token = get_jwt_token(account_name, config_manager)
    if jwt_token:
        existing_repo = get_repo_details(
            jwt_token, symbol, log, account_name, config_manager)
        if existing_repo:
            if log:
                log.warning(f"SAFEGUARD: Repo already exists for {symbol} (ID: {existing_repo['id']}). Skipping new repo creation.")
            return {"status": "skipped", "reason": "repo_exists", "existing_repo_id": existing_repo['id']}
    
    # Generate a unique repo client order ID
    clordid = generate_repo_clordid()
    
    # Create repo order data
    order_data = {
        "side": side,
        "price": float(interest_rate),
        "custodianId": custodian_id,
        "symbol": symbol,
        "currency": base_currency,
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
        hmac.new(api_secret.encode(), string_to_sign.encode(), hashlib.sha256).digest()
    ).decode()
    
    # Set up headers
    headers = {
        'api-key': api_key,
        'api-sign': signature,
        'Content-Type': 'application/json'
    }
    
    if log:
        log.debug(f"Repo order request: {url}")
        log.debug(f"Repo order body: {body}")
    
    # Make the API request
    try:
        response = requests.post(url, headers=headers, data=body)
        
        if response.ok:
            if log:
                log.info(f"Repo order placed successfully with API key auth")
            if response.text:
                return response.json()
            return {}
        else:
            log.error(f"Repo order failed with API key auth: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        log.error(f"Error placing repo order with API key auth: {str(e)}")
        return None

def close_repo(jwt_token=None, symbol=None, logger=None, api_key=None, api_secret=None,
               account_name=None, config_manager=None):
    """
    Close a repo contract
    
    Args:
        jwt_token: JWT token for authentication
        symbol: Repo symbol (e.g., "BTC/USDC110")
        logger: Logger object for logging
        api_key: API key (not used in this implementation)
        api_secret: API secret (not used in this implementation)
        account_name: Optional account name to get settings from
        config_manager: Optional ConfigurationManager instance
    
    Returns:
        bool: True if successfully closed
    """
    log = logger or logging.getLogger('trading_utils')
    
    if log:
        log.info(f"Attempting to close repo for {symbol}")
    
    # Get base_url from config if provided
    if config_manager and account_name:
        credentials = config_manager.get_account_credentials(account_name)
        base_url = credentials.get('api_base_url')
    else:
        base_url = os.getenv("API_BASE_URL")
    
    # Get JWT token if not provided
    if not jwt_token:
        jwt_token = get_jwt_token(account_name, config_manager)
        if not jwt_token:
            log.error("Failed to get JWT token")
            return False
    
    # Set headers
    headers = {
        "Authorization": jwt_token,
        "Content-Type": "application/json",
        "User-Agent": "python-requests/2.28.1"
    }
    
    # Get repo details first
    repo_details = get_repo_details(
        jwt_token, symbol, log, account_name, config_manager)
    
    if not repo_details:
        if log:
            log.warning(f"No open repo found for {symbol} to close")
        return True  # Consider it a success if there's no repo to close
    
    # Get the repo ID and set up the close request
    repo_id = repo_details["id"]
    
    # Create a new event ID for closing
    close_event_id = f"closeEvent{int(time.time())}"
    
    # IMPORTANT: Use GET with URL parameters, not POST with JSON payload
    if not base_url.endswith('/'):
        base_url += '/'
    
    # Using GET with URL parameters as in the working reference code
    close_url = f"{base_url}rest/repocontract/close?repoContractId={repo_id}&eventId={close_event_id}"
    
    try:
        # Use GET with URL parameters
        close_response = requests.get(
            url=close_url, 
            headers=headers
        )
        
        if log:
            log.debug(f"Close repo response: {close_response.status_code}")
            log.debug(f"Close repo response body: {close_response.text}")
        
        if not close_response.ok:
            if log:
                log.error(f"Failed to close repo: {close_response.text}")
            return False
        
        if log:
            log.info(f"Successfully closed repo for {symbol}")
        
        return True
        
    except Exception as e:
        if log:
            log.error(f"Error in close_repo: {str(e)}")
        return False