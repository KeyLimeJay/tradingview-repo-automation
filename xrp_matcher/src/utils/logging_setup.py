"""
Logging setup utilities for the XRP Matcher.
"""
import logging
from config.config import LOG_LEVEL, LOG_FILE

def setup_logging():
    """Configure the logging system."""
    logging.basicConfig(
        level=LOG_LEVEL,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )
    
    # Set WebSocket library's logger to DEBUG
    logging.getLogger('websocket').setLevel(logging.DEBUG)
    
    return logging.getLogger('xrp_matcher')
