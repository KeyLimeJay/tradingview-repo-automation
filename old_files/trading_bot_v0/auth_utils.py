#!/usr/bin/env python3
import requests
import json
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger('auth_utils')

def get_jwt_token(username=None, password=None, auth_code=None, base_url=None):
    """
    Generate JWT token by logging in with credentials
    
    Args:
        username: API username
        password: API password
        auth_code: Authentication code
        base_url: Base URL for the API
        
    Returns:
        JWT token or None if failed
    """
    # Use environment variables if parameters are not provided
    username = username or os.getenv('API_USERNAME')
    password = password or os.getenv('API_PASSWORD')
    auth_code = auth_code or os.getenv('API_CODE')
    base_url = base_url or os.getenv('API_BASE_URL')
    
    # Ensure base_url ends with a slash
    if not base_url.endswith('/'):
        base_url += '/'
    
    method = "POST"
    endpoint = "sso/api/login"
    url = base_url + endpoint
    
    payload = {
        "code": auth_code,
        "password": password,
        "redirectTo": base_url,
        "username": username
    }
    
    body = json.dumps(payload)
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': '*/*'
    }
    
    try:
        logger.debug(f"Attempting to get JWT token for {username} from {url}")
        response = requests.post(url, headers=headers, data=body)
        
        if not response.ok:
            logger.error(f"Failed to get JWT token: {response.status_code} - {response.text}")
            return None
        
        # Extract JWT token from Authorization header - exactly like reference code
        token = response.headers.get("authorization")
        
        if not token:
            logger.error("No authorization token found in response headers")
            return None
        
        logger.info("Successfully obtained JWT token")
        return token
        
    except Exception as e:
        logger.error(f"Error getting JWT token: {str(e)}")
        return None

if __name__ == "__main__":
    # Test the function
    logging.basicConfig(level=logging.DEBUG)
    token = get_jwt_token()
    print(f"Token: {token[:30]}..." if token else "Failed to get token")