# test_connection_pyfix.py
import socket
import logging
import sys
import os
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('FixConnectionTest')

class FixClient:
    def __init__(self, host, port, sender_comp_id, target_comp_id):
        self.host = host
        self.port = int(port)
        self.sender_comp_id = sender_comp_id
        self.target_comp_id = target_comp_id
        self.socket = None
        self.seq_num = 1
        self.connected = False
        self.SOH = chr(1)  # Start of Header character (ASCII 1)
        
    def connect(self, timeout=10):
        """Connect to the FIX server"""
        logger.info(f"Connecting to {self.host}:{self.port}...")
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(timeout)
            self.socket.connect((self.host, self.port))
            self.connected = True
            logger.info(f"Connected to {self.host}:{self.port}")
            return True
        except socket.error as e:
            logger.error(f"Failed to connect: {e}")
            self.connected = False
            return False
            
    def disconnect(self):
        """Disconnect from the server"""
        if self.connected and self.socket:
            try:
                self.socket.close()
                logger.info("Disconnected from server")
            except Exception as e:
                logger.error(f"Error disconnecting: {e}")
            self.connected = False
            
    def create_fix_message(self, msg_type, fields):
        """Create a FIX message with header, body, and trailer"""
        # Create sending time
        now = time.strftime("%Y%m%d-%H:%M:%S.000", time.gmtime())
        
        # Standard header fields
        header = (
            f"8=FIX.4.4{self.SOH}"  # BeginString
            f"9=000{self.SOH}"      # BodyLength (placeholder)
            f"35={msg_type}{self.SOH}"  # MsgType
            f"49={self.sender_comp_id}{self.SOH}"  # SenderCompID
            f"56={self.target_comp_id}{self.SOH}"  # TargetCompID
            f"34={self.seq_num}{self.SOH}"  # MsgSeqNum
            f"52={now}{self.SOH}"  # SendingTime
        )
        
        # Increment sequence number for next message
        self.seq_num += 1
        
        # Body
        body = ""
        for tag, value in fields.items():
            body += f"{tag}={value}{self.SOH}"
            
        # Message without checksum
        msg_without_checksum = header + body
        
        # Calculate body length (excluding checksum)
        body_length = len(msg_without_checksum) - len(f"8=FIX.4.4{self.SOH}9=000{self.SOH}")
        
        # Replace placeholder body length
        msg_without_checksum = msg_without_checksum.replace(f"9=000{self.SOH}", f"9={body_length:03d}{self.SOH}")
        
        # Calculate checksum
        checksum = sum(ord(c) for c in msg_without_checksum) % 256
        
        # Append checksum
        full_message = msg_without_checksum + f"10={checksum:03d}{self.SOH}"
        
        return full_message
        
    def send_logon(self, heartbeat_interval=30):
        """Send a logon message"""
        if not self.connected:
            logger.error("Not connected to server")
            return False
            
        logger.info("Sending logon message...")
        
        # Logon fields
        fields = {
            "98": "0",     # EncryptMethod (0 = None)
            "108": str(heartbeat_interval),  # HeartBtInt
            "141": "Y"     # ResetSeqNumFlag (Y = Yes)
        }
        
        # Create and send logon message
        logon_msg = self.create_fix_message("A", fields)
        
        try:
            self.socket.sendall(logon_msg.encode('utf-8'))
            logger.info(f"Sent logon message: {logon_msg.replace(self.SOH, '|')}")
            return True
        except socket.error as e:
            logger.error(f"Failed to send logon message: {e}")
            self.connected = False
            return False
            
    def receive_message(self, timeout=5):
        """Receive a FIX message"""
        if not self.connected:
            logger.error("Not connected to server")
            return None
            
        try:
            self.socket.settimeout(timeout)
            buffer = b""
            
            # Read data until we get a complete message
            while True:
                data = self.socket.recv(4096)
                if not data:
                    logger.error("Connection closed by server")
                    self.connected = False
                    return None
                    
                buffer += data
                
                # Check if we have a complete message (ending with 10=xxx<SOH>)
                if b"\x0110=" in buffer and buffer.find(b"\x01", buffer.rfind(b"\x0110=") + 4) != -1:
                    break
                    
            # Decode the message
            msg = buffer.decode('utf-8')
            logger.info(f"Received message: {msg.replace(self.SOH, '|')}")
            
            # Extract message type
            msg_type_start = msg.find("35=") + 3
            msg_type_end = msg.find(self.SOH, msg_type_start)
            msg_type = msg[msg_type_start:msg_type_end]
            
            return {
                "full_message": msg,
                "msg_type": msg_type
            }
            
        except socket.timeout:
            logger.warning("Timeout while waiting for response")
            return None
        except socket.error as e:
            logger.error(f"Error receiving message: {e}")
            self.connected = False
            return None

def test_fix_connection():
    """Test connection to a FIX server"""
    # Get connection details from environment
    host = os.getenv("BOSONIC_HOST", "localhost")
    port = os.getenv("BOSONIC_PORT", "18305")
    sender_comp_id = os.getenv("BOSONIC_SENDER_COMP_ID", "KUROSHIO_PRICE_FEED_MD")
    target_comp_id = os.getenv("BOSONIC_TARGET_COMP_ID", "OGG_MD_KUROSHIO_PRICE_FEED")
    
    logger.info(f"Testing FIX connection to {host}:{port}")
    logger.info(f"Using SenderCompID={sender_comp_id}, TargetCompID={target_comp_id}")
    
    # Create FIX client
    client = FixClient(host, port, sender_comp_id, target_comp_id)
    
    try:
        # Connect to server
        if not client.connect():
            logger.error("Failed to connect to server")
            return False
            
        # Send logon message
        if not client.send_logon():
            logger.error("Failed to send logon message")
            return False
            
        # Wait for logon response
        response = client.receive_message()
        
        if response and response["msg_type"] == "A":
            logger.info("Logon successful!")
            return True
        else:
            logger.error("Logon failed or no response received")
            return False
            
    finally:
        # Disconnect
        client.disconnect()
        
def test_basic_connectivity():
    """Simple test to check if the server is reachable"""
    # Get connection details from environment
    host = os.getenv("BOSONIC_HOST", "localhost")
    port = int(os.getenv("BOSONIC_PORT", "18305"))
    
    logger.info(f"Testing basic connectivity to {host}:{port}")
    
    try:
        # Create socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        
        # Try to connect
        result = sock.connect_ex((host, port))
        
        if result == 0:
            logger.info(f"Successfully connected to {host}:{port}")
            return True
        else:
            logger.error(f"Failed to connect to {host}:{port}. Error code: {result}")
            return False
            
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False
        
    finally:
        # Close socket
        sock.close()

if __name__ == "__main__":
    # First test basic connectivity
    if test_basic_connectivity():
        # If basic connectivity is successful, test FIX connection
        success = test_fix_connection()
        sys.exit(0 if success else 1)
    else:
        sys.exit(1)