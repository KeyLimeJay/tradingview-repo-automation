#!/usr/bin/env python3
import os
import logging
import json
import requests
import hmac
import hashlib
import base64
import time

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('eth_repo_bot')

class SimpleRepoBot:
    """Simple bot to demonstrate opening a repo for ETH"""
    
    def __init__(self):
        self.api_key = os.getenv('API_KEY')
        self.api_secret = os.getenv('API_SECRET')
        self.custodian_id = os.getenv('CUSTODIAN_ID')
        self.api_url = os.getenv('API_URL')
        self.api_base_url = os.getenv('API_BASE_URL')
        self.api_username = os.getenv('API_USERNAME')
        self.api_password = os.getenv('API_PASSWORD')
        self.api_code = os.getenv('API_CODE')
        
        # Validate required credentials
        self._validate_credentials()
    
    def _validate_credentials(self):
        """Validate that all required credentials are available"""
        required_vars = [
            ('API_KEY', self.api_key),
            ('API_SECRET', self.api_secret),
            ('CUSTODIAN_ID', self.custodian_id),
            ('API_URL', self.api_url),
            ('API_BASE_URL', self.api_base_url),
            ('API_USERNAME', self.api_username),
            ('API_PASSWORD', self.api_password),
            ('API_CODE', self.api_code)
        ]
        
        missing = [name for name, value in required_vars if not value]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    def get_jwt_token(self):
        """Get JWT token for API authentication"""
        logger.info("Getting JWT token...")
        
        # Ensure base_url ends with a slash
        base_url = self.api_base_url
        if not base_url.endswith('/'):
            base_url += '/'
        
        url = f"{base_url}sso/api/login"
        
        payload = {
            "code": self.api_code,
            "password": self.api_password,
            "redirectTo": base_url,
            "username": self.api_username,
            "email": self.api_username  # Using username as email
        }
        
        headers = {
            'Content-Type': 'application/json',
            'Accept': '*/*'
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
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
    
    def check_existing_repo(self, symbol):
        """Check if a repo already exists for the given symbol"""
        logger.info(f"Checking if repo already exists for {symbol}...")
        
        jwt_token = self.get_jwt_token()
        if not jwt_token:
            logger.error("Failed to get JWT token")
            return None
        
        # Set headers
        headers = {
            "Authorization": jwt_token,
            "Content-Type": "application/json",
            "User-Agent": "python-requests/2.28.1"
        }
        
        # Ensure base_url ends with a slash
        base_url = self.api_base_url
        if not base_url.endswith('/'):
            base_url += '/'
        
        # Create payload for POST request
        payload = {
            "userId": self.api_username,
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
            
            logger.debug(f"Repo details response: {repo_response.status_code}")
            
            if not repo_response.ok:
                logger.error(f"Failed to get repo details: {repo_response.text}")
                return None
            
            repo_data = repo_response.json()
            
            if not repo_data.get("content") or len(repo_data["content"]) == 0:
                logger.info(f"No open repo found for {symbol}")
                return None
            
            # Get the first open repo
            repo_contract = repo_data["content"][0]
            repo_id = repo_contract.get("id")
            event_id = repo_contract.get("eventId")
            
            if not repo_id:
                logger.error("No repo ID found in response")
                return None
            
            logger.info(f"Found existing repo with ID: {repo_id}, Event ID: {event_id}")
                
            return {
                "id": repo_id,
                "eventId": event_id
            }
            
        except Exception as e:
            logger.error(f"Error retrieving repo details: {str(e)}")
            return None
    
    def generate_repo_clordid(self):
        """Generate a unique client order ID for repo orders"""
        timestamp = time.strftime("%Y%m%d%H%M%S")
        random_suffix = ''.join([chr(ord('a') + int(random.random() * 26)) for _ in range(12)])
        return f"WEB:{random_suffix}-{timestamp}"
    
    def open_eth_repo(self, quantity=0.1, interest_rate=10.0):
        """Open a repo for ETH using API key authentication"""
        symbol = "ETH/USDC110"
        logger.info(f"Attempting to open repo for {symbol}...")
        
        # Check if repo already exists
        existing_repo = self.check_existing_repo(symbol)
        if existing_repo:
            logger.warning(f"Repo already exists for {symbol} (ID: {existing_repo['id']}). Skipping.")
            return {"status": "skipped", "reason": "repo_exists", "existing_repo_id": existing_repo['id']}
        
        # Set up URL
        url = f"{self.api_url}/rest/orders"
        endpoint = "/rest/orders"
        
        # Generate a unique repo client order ID
        clordid = self.generate_repo_clordid()
        
        # Create repo order data
        order_data = {
            "side": "BID",  # BID for borrowing
            "price": float(interest_rate),
            "custodianId": self.custodian_id,
            "symbol": symbol,
            "currency": "ETH",
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
            hmac.new(self.api_secret.encode(), string_to_sign.encode(), hashlib.sha256).digest()
        ).decode()
        
        # Set up headers
        headers = {
            'api-key': self.api_key,
            'api-sign': signature,
            'Content-Type': 'application/json'
        }
        
        logger.debug(f"Repo order request: {url}")
        logger.debug(f"Repo order body: {body}")
        
        # Make the API request
        try:
            response = requests.post(url, headers=headers, data=body, timeout=30)
            
            if response.ok:
                logger.info(f"Repo order placed successfully!")
                if response.text:
                    return response.json()
                return {"status": "success", "message": "Repo order placed successfully"}
            else:
                logger.error(f"Repo order failed: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error placing repo order: {str(e)}")
            return None

def run():
    """Main function to run the repo test"""
    try:
        # Create bot instance
        bot = SimpleRepoBot()
        
        # Open ETH repo
        result = bot.open_eth_repo(quantity=0.1, interest_rate=10.0)
        
        if result:
            if result.get("status") == "skipped":
                logger.info(f"Skipped opening repo: {result.get('reason')}")
            else:
                logger.info("Repo opened successfully!")
                logger.info(f"Response: {json.dumps(result, indent=2)}")
        else:
            logger.error("Failed to open repo")
            
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    run()