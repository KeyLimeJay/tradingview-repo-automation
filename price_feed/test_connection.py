#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import time
import os
import logging
import quickfix as fix
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BosionicFIXTest")

class TestApplication(fix.Application):
    """Simple QuickFIX application to test FIX connection"""
    
    def __init__(self):
        super(TestApplication, self).__init__()
        self.logger = logging.getLogger("BosionicFIXTest.Application")
        self.session_id = None
        self.logged_on = False
        
    def onCreate(self, sessionID):
        """Called when FIX session is created"""
        self.logger.info(f"Session created: {sessionID}")
        self.session_id = sessionID
        
    def onLogon(self, sessionID):
        """Called when FIX session is established"""
        self.logger.info(f"Successfully logged on: {sessionID}")
        self.logged_on = True
        
    def onLogout(self, sessionID):
        """Called when FIX session is terminated"""
        self.logger.info(f"Session logout: {sessionID}")
        self.logged_on = False
        
    def toAdmin(self, message, sessionID):
        """Called when administrative message is sent to target"""
        msg_type = message.getHeader().getField(fix.MsgType().getField())
        if msg_type == fix.MsgType_Logon:
            self.logger.info("Sending Logon message")
            
    def fromAdmin(self, message, sessionID):
        """Called when administrative message is received from target"""
        msg_type = message.getHeader().getField(fix.MsgType().getField())
        if msg_type == fix.MsgType_Logon:
            self.logger.info("Received Logon message")
        
    def toApp(self, message, sessionID):
        """Called when application message is sent to target"""
        self.logger.info(f"Sending application message: {message}")
        
    def fromApp(self, message, sessionID):
        """Called when application message is received from target"""
        self.logger.info(f"Received application message: {message}")

def test_fix_connection():
    """Test connection to FIX server using QuickFIX"""
    
    # Get connection details from environment
    host = os.getenv("BOSONIC_HOST", "localhost")
    port = os.getenv("BOSONIC_PORT", "18305")
    sender_comp_id = os.getenv("BOSONIC_SENDER_COMP_ID", "KUROSHIO_PRICE_FEED_MD")
    target_comp_id = os.getenv("BOSONIC_TARGET_COMP_ID", "OGG_MD_KUROSHIO_PRICE_FEED")
    
    logger.info(f"Testing FIX connection to {host}:{port}")
    logger.info(f"Using SenderCompID={sender_comp_id}, TargetCompID={target_comp_id}")
    
    # Create settings
    settings = fix.SessionSettings()
    
    # Configure session
    session = fix.Dictionary()
    session.setString("ConnectionType", "initiator")
    session.setString("ReconnectInterval", "5")
    session.setString("HeartBtInt", "30")  # Heartbeat interval in seconds
    session.setString("StartTime", "00:00:00")
    session.setString("EndTime", "23:59:59")
    session.setString("FileLogPath", "logs")
    session.setString("FileStorePath", "store")
    session.setString("ValidateUserDefinedFields", "N")
    session.setString("UseDataDictionary", "N")  # Set to Y if you have a data dictionary file
    session.setString("ResetOnLogon", "Y")
    
    # Connection details
    session.setString("SocketConnectHost", host)
    session.setString("SocketConnectPort", port)
    session.setString("SenderCompID", sender_comp_id)
    session.setString("TargetCompID", target_comp_id)
    session.setString("BeginString", "FIX.4.4")  # According to Bosonic docs
    
    # Add the session to settings
    settings.set(session)
    
    # Create the application instance
    application = TestApplication()
    
    # Create FIX components
    try:
        store_factory = fix.FileStoreFactory(settings)
        log_factory = fix.FileLogFactory(settings)
        initiator = fix.SocketInitiator(application, store_factory, settings, log_factory)
        
        logger.info("Starting FIX initiator...")
        initiator.start()
        
        # Wait for logon
        max_wait = 10  # seconds
        start_time = time.time()
        while time.time() - start_time < max_wait:
            if application.logged_on:
                logger.info("Successfully authenticated with FIX server!")
                break
            time.sleep(0.5)
        else:
            logger.error(f"Failed to logon within {max_wait} seconds.")
        
        # Allow some time for messages to be exchanged
        time.sleep(2)
        
        # Clean shutdown
        logger.info("Test complete, stopping initiator...")
        initiator.stop()
        logger.info("FIX connection test finished.")
        
        return application.logged_on
        
    except fix.ConfigError as e:
        logger.error(f"Configuration error: {e}")
        return False
    except fix.RuntimeError as e:
        logger.error(f"Runtime error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

if __name__ == "__main__":
    # Create logs and store directories if they don't exist
    for directory in ["logs", "store"]:
        if not os.path.exists(directory):
            os.makedirs(directory)
    
    # Run the test
    success = test_fix_connection()
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)