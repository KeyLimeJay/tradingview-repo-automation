#auth.py
import hmac
import hashlib
import uuid
import time
import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from config.settings import API_KEY, API_SECRET

def get_auth_headers():
    """
    Generate authentication headers required for Bitstamp API.
    """
    # Generate timestamp (milliseconds since epoch)
    timestamp = str(int(round(time.time() * 1000)))
    
    # Generate a unique nonce (required for each request)
    nonce = str(uuid.uuid4())
    
    # Create the signature message
    message = f"BITSTAMP {API_KEY}wsswww.bitstamp.netNone{nonce}{timestamp}v2"
    message = message.encode('utf-8')  # Convert to bytes
    
    # Create HMAC-SHA256 signature
    signature = hmac.new(API_SECRET.encode('utf-8'), msg=message, digestmod=hashlib.sha256).hexdigest()
    
    # Return all required headers
    return {
        "X-Auth": f"BITSTAMP {API_KEY}",
        "X-Auth-Signature": signature,
        "X-Auth-Nonce": nonce,
        "X-Auth-Timestamp": timestamp,
        "X-Auth-Version": "v2"
    }