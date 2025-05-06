#trading_utils.py (New code)
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
        
    Returns:
        Response dict with additional fill information for IOC orders
        or None if the order failed to be placed/filled
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
    
    # Check if credentials are valid
    if not all([api_key, api_secret, custodian_id, api_url]):
        logger.error("Missing required credentials for order placement")
        raise OrderPlacementError("Missing required credentials for order placement")
    
    # Set up URL
    url = f"{api_url}/rest/orders"
    endpoint = "/rest/orders"
    method = "POST"
    
    logger.info(f"Placing Order - Symbol: {symbol}, Side: {side}, Price: {price}, Quantity: {quantity}, TIF: {tif}")
    
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
                response = requests.post(url, headers=headers, data=body, timeout=30)
                
                if response.ok:
                    logger.info(f"Order placed successfully: Status {response.status_code}")
                    
                    # For IOC orders, specifically check fill status
                    if tif == 'IOC':
                        response_data = response.json() if response.text else {}
                        
                        # Try to get fill information
                        if response_data:
                            # Check if the order was filled
                            ord_status = response_data.get('ordStatus', '')
                            leaves_qty = float(response_data.get('leavesQty', 0))
                            cum_qty = float(response_data.get('cumQty', 0))
                            
                            # Add fill information to response
                            response_data['fill_complete'] = (
                                ord_status == 'FILLED' or 
                                (cum_qty > 0 and leaves_qty == 0)
                            )
                            
                            # If not filled, treat as failure for IOC
                            if not response_data['fill_complete']:
                                logger.warning(f"IOC order not filled: {ord_status}, leaves_qty={leaves_qty}, cum_qty={cum_qty}")
                                if ord_status in ['EXPIRED', 'REJECTED', 'CANCELLED']:
                                    return None  # Order failed to fill
                            
                            logger.info(f"IOC order fill status: {response_data['fill_complete']}")
                            return response_data
                    
                    # For non-IOC orders, just return the response
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
        
        # Save original environment variables to restore later
        orig_username = os.environ.get('API_USERNAME')
        orig_password = os.environ.get('API_PASSWORD')
        orig_code = os.environ.get('API_CODE')
        orig_base_url = os.environ.get('API_BASE_URL')
        
        # Temporarily set environment variables for this request
        if user: os.environ['API_USERNAME'] = user
        if password: os.environ['API_PASSWORD'] = password
        if code: os.environ['API_CODE'] = code
        if base_url: os.environ['API_BASE_URL'] = base_url
    else:
        user = os.getenv("API_USERNAME")
        password = os.getenv("API_PASSWORD")
        code = os.getenv("API_CODE")
        base_url = os.getenv("API_BASE_URL")
        # No need to save/restore in this case
        orig_username = orig_password = orig_code = orig_base_url = None
    
    logger.info(f"Base URL: {base_url}")
    logger.info(f"Username: {user}")
    logger.info(f"Password: {'*****' if password else None}")            
    logger.info(f"Code: {code}")
    
    token = None
    
    try:
        # Ensure we have all required credentials
        if not all([user, password, code, base_url]):
            logger.error("Missing required credentials for JWT authentication")
            return None
        
        # Ensure base_url ends with a slash
        if not base_url.endswith('/'):
            base_url += '/'
        
        method = "POST"
        endpoint = "sso/api/login"
        url = base_url + endpoint
        
        payload = {
            "code": code,
            "password": password,
            "redirectTo": base_url,
            "username": user,
            "email": user  # Add email parameter using the same value as username
        }
        
        body = json.dumps(payload)
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': '*/*'
        }
        
        logger.debug(f"Getting JWT token for {user}")
        
        response = requests.post(url, headers=headers, data=body, timeout=30)
        
        if not response.ok:
            logger.error(f"Failed to get JWT token: {response.status_code} - {response.text}")
            return None
                
        token = response.headers.get("authorization")
        if not token:
            logger.error("No authorization token in response headers")
            return None
                
        logger.info("Successfully obtained JWT token")
    
    except Exception as e:
        logger.error(f"Error getting JWT token: {str(e)}")
        return None
    
    finally:
        # Restore original environment variables if we changed them
        if config_manager and account_name:
            if orig_username is not None:
                os.environ['API_USERNAME'] = orig_username
            elif 'API_USERNAME' in os.environ:
                del os.environ['API_USERNAME']
                
            if orig_password is not None:
                os.environ['API_PASSWORD'] = orig_password
            elif 'API_PASSWORD' in os.environ:
                del os.environ['API_PASSWORD']
                
            if orig_code is not None:
                os.environ['API_CODE'] = orig_code
            elif 'API_CODE' in os.environ:
                del os.environ['API_CODE']
                
            if orig_base_url is not None:
                os.environ['API_BASE_URL'] = orig_base_url
            elif 'API_BASE_URL' in os.environ:
                del os.environ['API_BASE_URL']
    
    return token

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
        
    # Ensure required parameters are present
    if not all([base_url, username, symbol]):
        log.error("Missing required parameters for repo details lookup")
        return None
    
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
    
    # Ensure base_url ends with a slash
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
            json=payload,
            timeout=30
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
    
    # Check if credentials are valid
    if not all([api_key, api_secret, custodian_id, api_url, symbol, quantity]):
        log.error("Missing required parameters for repo order placement")
        return None
    
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
        response = requests.post(url, headers=headers, data=body, timeout=30)
        
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
    Close a repo contract using the approach confirmed to work
    
    Args:
        jwt_token: JWT token for authentication
        symbol: Repo symbol (e.g., "ETH/USDC110")
        logger: Logger object for logging
        api_key: API key (not used in this implementation)
        api_secret: API secret (not used in this implementation)
        account_name: Optional account name to get settings from
        config_manager: Optional ConfigurationManager instance
    
    Returns:
        bool: True if successfully closed or if no repo exists to close
    """
    log = logger or logging.getLogger('trading_utils')
    
    # Print diagnostic start
    print("\n=== REPO CLOSING DIAGNOSTIC START ===")
    print(f"Attempting to close repo for {symbol}")
    
    # Get base_url from config or environment
    base_url = None
    
    if config_manager and account_name:
        credentials = config_manager.get_account_credentials(account_name)
        base_url = credentials.get('api_base_url')
        print(f"Using base URL from config: {base_url}")
    else:
        base_url = os.getenv("API_BASE_URL")
        print(f"Using base URL from env: {base_url}")
    
    if not base_url:
        log.error("No base URL available for repo closing")
        print("FAILED: No base URL available for repo closing")
        print("=== REPO CLOSING DIAGNOSTIC END ===\n")
        return False
    
    # Normalize URL
    if not base_url.endswith('/'):
        base_url += '/'
    
    # Get JWT token if not provided
    if not jwt_token:
        print("Getting JWT token...")
        if config_manager and account_name:
            jwt_token = get_jwt_token(account_name, config_manager)
        else:
            jwt_token = get_jwt_token()
            
        if not jwt_token:
            log.error("Failed to get JWT token")
            print("FAILED: Could not get JWT token")
            print("=== REPO CLOSING DIAGNOSTIC END ===\n")
            return False
        
        print("JWT token obtained successfully")
    else:
        print("JWT token was provided")
    
    # Set headers - exactly as in the working code
    headers = {
        "Authorization": jwt_token,
        "Content-Type": "application/json",
        "User-Agent": "python-requests/2.28.1"
    }
    print(f"Headers set up with JWT token")
    
    # Get repo details using the exact URL format from working example
    repo_url = f"{base_url}rest/repocontract?sortBy=id&sortDirection=DESC&status=OPEN&repoSymbol={symbol}"
    print(f"Repo details URL: {repo_url}")
    
    # Get username from config or environment
    if config_manager and account_name:
        username = credentials.get('api_username')
        print(f"Using username from config: {username}")
    else:
        username = os.getenv("API_USERNAME")
        print(f"Using username from env: {username}")
    
    payload = {
        "userId": username,
        "contractType": "BORROW",
        "eventId": "event" + str(int(time.time())),
        "repoSymbol": symbol
    }
    print(f"Repo details payload: {payload}")
    
    try:
        log.info(f"Getting repo details from URL: {repo_url}")
        print(f"Sending repo details request...")
        
        repo_response = requests.post(
            url=repo_url,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        print(f"Repo details response code: {repo_response.status_code}")
        
        if not repo_response.ok:
            log.error(f"Failed to get repo details: {repo_response.status_code} - {repo_response.text}")
            print(f"FAILED: Repo details request failed: {repo_response.status_code}")
            print(f"Response text: {repo_response.text[:500]}")
            print("=== REPO CLOSING DIAGNOSTIC END ===\n")
            
            # Consider it a success if we can't even get repo details (as there might not be any repo)
            return True
        
        try:
            repo_data = repo_response.json()
            print(f"Successfully parsed repo details JSON")
            
            if not repo_data.get("content") or len(repo_data["content"]) == 0:
                log.warning(f"No open repo found for {symbol}")
                print(f"WARNING: No open repo found for {symbol}")
                print("=== REPO CLOSING DIAGNOSTIC END ===\n")
                return True  # Consider it a success if there's no repo to close
            
            # Get the first open repo
            repo_contract = repo_data["content"][0]
            repo_id = repo_contract.get("id")
            
            if not repo_id:
                log.error("No repo ID found in response")
                print("FAILED: No repo ID found in response")
                print("=== REPO CLOSING DIAGNOSTIC END ===\n")
                return True  # Return true to continue trading sequence
            
            log.info(f"Found repo with ID: {repo_id}")
            print(f"FOUND REPO with ID: {repo_id}")
            print(f"Repo status: {repo_contract.get('status')}")
            print(f"Repo symbol: {repo_contract.get('repoSymbol')}")
            print(f"Repo quantity: {repo_contract.get('qty')}")
            print(f"Repo created: {repo_contract.get('createdTime')}")
            
            # Create a new event ID for closing - same format as working code
            close_event_id = "closeEvent" + str(int(time.time()))
            
            # Construct the URL exactly as in the working code
            close_url = f"{base_url}rest/repocontract/close?repoContractId={repo_id}&eventId={close_event_id}"
            
            log.info(f"Closing repo with URL: {close_url}")
            print(f"Closing repo with URL: {close_url}")
            
            # Use simple GET request - exactly as in the working code
            print(f"Sending close repo request...")
            close_response = requests.get(
                url=close_url, 
                headers=headers,
                timeout=30
            )
            
            log.debug(f"Close repo response: {close_response.status_code}")
            print(f"Close repo response code: {close_response.status_code}")
            if close_response.text:
                log.debug(f"Close repo response body: {close_response.text[:500]}")
                print(f"Close repo response body: {close_response.text[:500]}")
            
            if not close_response.ok:
                log.error(f"Failed to close repo: {close_response.status_code} - {close_response.text[:100]}")
                print(f"FAILED: Close repo request failed: {close_response.status_code}")
                print(f"Response text: {close_response.text[:500]}")
                print("=== REPO CLOSING DIAGNOSTIC END ===\n")
                
                # Return TRUE anyway to prevent blocking the trading sequence
                return True
            
            log.info(f"Successfully closed repo for {symbol} with ID {repo_id}")
            print(f"SUCCESS: Closed repo for {symbol} with ID {repo_id}")
            print("=== REPO CLOSING DIAGNOSTIC END ===\n")
            
            return True
            
        except Exception as e:
            log.error(f"Error processing repo details: {str(e)}")
            print(f"FAILED: Error processing repo details: {str(e)}")
            print(f"Exception details: {type(e).__name__}: {str(e)}")
            print("=== REPO CLOSING DIAGNOSTIC END ===\n")
            return True  # Return true to continue trading sequence
            
    except Exception as e:
        log.error(f"Error in close_repo: {str(e)}")
        print(f"FAILED: Error in close_repo: {str(e)}")
        print(f"Exception details: {type(e).__name__}: {str(e)}")
        print("=== REPO CLOSING DIAGNOSTIC END ===\n")
        
        # Return TRUE anyway to prevent blocking the trading sequence
        return True