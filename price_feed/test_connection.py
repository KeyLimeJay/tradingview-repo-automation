import socket
import os
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("ConnectionTest")

def test_connection():
    """Test connection to the Bosonic FIX server"""
    # Load environment variables
    load_dotenv()
    
    host = os.getenv("BOSONIC_HOST")
    port = int(os.getenv("BOSONIC_PORT", "18305"))
    
    logger.info(f"Testing connection to {host}:{port}")
    
    try:
        # Create socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        
        # Try to connect
        result = sock.connect_ex((host, port))
        
        if result == 0:
            logger.info(f"Successfully connected to {host}:{port}")
            return True
        else:
            logger.error(f"Failed to connect to {host}:{port}. Error code: {result}")
            return False
    
    except socket.gaierror:
        logger.error(f"Hostname could not be resolved: {host}")
        return False
    
    except socket.error as e:
        logger.error(f"Socket error: {e}")
        return False
    
    finally:
        # Close the socket
        sock.close()

if __name__ == "__main__":
    test_connection()