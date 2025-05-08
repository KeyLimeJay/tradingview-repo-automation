import socket
import time
import datetime
import pandas as pd
import threading
import re
import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
log_level = os.getenv("LOG_LEVEL", "INFO")
log_file = os.getenv("LOG_FILE", "bosonic_fix_client.log")

numeric_level = getattr(logging, log_level.upper(), None)
if not isinstance(numeric_level, int):
    raise ValueError(f"Invalid log level: {log_level}")

logging.basicConfig(
    level=numeric_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("BosionicClient")

class BosionicClient:
    def __init__(self, host, port, sender_comp_id, target_comp_id):
        """Initialize the Bosonic FIX client"""
        self.host = host
        self.port = int(port)
        self.sender_comp_id = sender_comp_id
        self.target_comp_id = target_comp_id
        self.socket = None
        self.seq_num = 1
        self.connected = False
        self.market_data = {}
        self.msg_buffer = ""
        self.listener_thread = None
        self.heartbeat_thread = None
        self.running = False
        self.SOH = chr(1)  # Start of Header character (ASCII 1)
        logger.info(f"Initialized client for {sender_comp_id} -> {target_comp_id}")
        
    def connect(self):
        """Establish connection to the server"""
        try:
            logger.info(f"Connecting to {self.host}:{self.port}")
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Set timeout for connection attempt
            self.socket.settimeout(10)  
            self.socket.connect((self.host, self.port))
            # Set back to blocking mode for normal operation
            self.socket.settimeout(None)  
            self.connected = True
            logger.info(f"Connected to {self.host}:{self.port}")
            
            # Send logon message
            self.send_logon()
            
            # Start listener thread
            self.running = True
            self.listener_thread = threading.Thread(target=self.listen)
            self.listener_thread.daemon = True
            self.listener_thread.start()
            
            # Start heartbeat thread
            self.heartbeat_thread = threading.Thread(target=self.heartbeat_sender)
            self.heartbeat_thread.daemon = True
            self.heartbeat_thread.start()
            
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            self.connected = False
            return False
            
    def disconnect(self):
        """Close the connection"""
        if self.connected:
            try:
                # Send logout message
                self.send_logout()
                self.running = False
                time.sleep(1)  # Allow time for listener thread to exit
                self.socket.close()
                logger.info("Disconnected from server")
            except Exception as e:
                logger.error(f"Error during disconnect: {e}")
        self.connected = False
        
    def create_header(self, msg_type):
        """Create FIX message header"""
        now = datetime.datetime.utcnow()
        sending_time = now.strftime("%Y%m%d-%H:%M:%S.%f")[:-3]
        
        header = (
            f"8=FIX.4.4{self.SOH}"  # BeginString
            f"9=0{self.SOH}"  # BodyLength (to be filled later)
            f"35={msg_type}{self.SOH}"  # MsgType
            f"49={self.sender_comp_id}{self.SOH}"  # SenderCompID
            f"56={self.target_comp_id}{self.SOH}"  # TargetCompID
            f"34={self.seq_num}{self.SOH}"  # MsgSeqNum
            f"52={sending_time}{self.SOH}"  # SendingTime
        )
        
        self.seq_num += 1
        return header
    
    def calculate_checksum(self, msg):
        """Calculate FIX message checksum"""
        checksum = sum(ord(c) for c in msg) % 256
        return f"10={checksum:03d}{self.SOH}"
    
    def calculate_body_length(self, msg):
        """Calculate FIX message body length"""
        # Find start of body (after tag 9) and end of body (before tag 10)
        start = msg.find(f"{self.SOH}35=")
        if start == -1:
            return 0
        
        end = msg.find(f"{self.SOH}10=")
        if end == -1:
            end = len(msg)
            
        return end - start
    
    def create_fix_message(self, msg_type, body):
        """Create a complete FIX message"""
        header = self.create_header(msg_type)
        msg = header + body
        
        # Calculate body length (excluding checksum)
        body_length = self.calculate_body_length(msg)
        
        # Replace placeholder body length
        msg = msg.replace(f"9=0{self.SOH}", f"9={body_length}{self.SOH}")
        
        # Add checksum
        msg += self.calculate_checksum(msg)
        
        return msg
    
    def send_message(self, message):
        """Send a FIX message to the server"""
        if not self.connected:
            logger.warning("Not connected to server")
            return False
            
        try:
            self.socket.sendall(message.encode('utf-8'))
            # Print message with SOH replaced by '|' for readability
            logger.debug("Sent: " + message.replace(self.SOH, '|'))
            return True
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            self.connected = False
            return False
    
    def send_logon(self):
        """Send a logon message"""
        body = (
            f"98=0{self.SOH}"  # EncryptMethod (0 = None)
            f"108=30{self.SOH}"  # HeartBtInt (30 seconds)
            f"141=Y{self.SOH}"  # ResetSeqNumFlag (Y = Yes)
        )
        
        msg = self.create_fix_message("A", body)
        return self.send_message(msg)
    
    def send_logout(self):
        """Send a logout message"""
        msg = self.create_fix_message("5", "")
        return self.send_message(msg)
    
    def send_heartbeat(self):
        """Send a heartbeat message"""
        msg = self.create_fix_message("0", "")
        return self.send_message(msg)
    
    def request_market_data(self, symbols):
        """Request market data for the given symbols"""
        if not isinstance(symbols, list):
            symbols = [symbols]
            
        for symbol in symbols:
            self.send_market_data_request(symbol)
    
    def send_market_data_request(self, symbol):
        """Send a market data request for a single symbol"""
        body = (
            f"262=MD_{self.seq_num}{self.SOH}"  # MDReqID (unique identifier)
            f"263=1{self.SOH}"  # SubscriptionRequestType (1 = Subscribe)
            f"264=0{self.SOH}"  # MarketDepth (0 = Full Book)
            f"265=0{self.SOH}"  # MDUpdateType (0 = Full Refresh)
            f"267=5{self.SOH}"  # NoMDEntryTypes (5 types)
            f"269=0{self.SOH}"  # MDEntryType (0 = Bid)
            f"269=1{self.SOH}"  # MDEntryType (1 = Offer)
            f"269=PB{self.SOH}"  # MDEntryType (PB = Peg Bid)
            f"269=PO{self.SOH}"  # MDEntryType (PO = Peg Offer)
            f"269=PM{self.SOH}"  # MDEntryType (PM = Mid Point)
            f"146=1{self.SOH}"  # NoRelatedSym (1 symbol)
            f"55={symbol}{self.SOH}"  # Symbol
        )
        
        msg = self.create_fix_message("V", body)
        success = self.send_message(msg)
        
        if success:
            logger.info(f"Market data request sent for {symbol}")
        
    def listen(self):
        """Listen for incoming messages"""
        logger.info("Listener thread started")
        while self.running:
            try:
                # Use a timeout to allow for checking self.running periodically
                self.socket.settimeout(1.0)
                try:
                    data = self.socket.recv(4096)
                    if not data:
                        logger.warning("Connection closed by server")
                        self.connected = False
                        break
                        
                    # Add received data to buffer
                    self.msg_buffer += data.decode('utf-8')
                    
                    # Process complete messages
                    self.process_buffer()
                except socket.timeout:
                    # This is expected - just check if we should continue running
                    continue
                
            except Exception as e:
                logger.error(f"Error receiving data: {e}")
                self.connected = False
                break
                
        logger.info("Listener thread stopped")
    
    def process_buffer(self):
        """Process the message buffer for complete FIX messages"""
        # Find the start of message
        while f"{self.SOH}10=" in self.msg_buffer:
            # Extract and process one message
            msg_end = self.msg_buffer.find(self.SOH, self.msg_buffer.find(f"{self.SOH}10=") + 4)
            
            if msg_end != -1:
                # Extract the complete message
                msg = self.msg_buffer[:msg_end + 1]
                self.msg_buffer = self.msg_buffer[msg_end + 1:]
                
                # Process the message
                self.handle_message(msg)
            else:
                # Incomplete message, wait for more data
                break
    
    def handle_message(self, msg):
        """Handle a FIX message"""
        # Log message with SOH replaced by '|' for readability
        logger.debug("Received: " + msg.replace(self.SOH, '|'))
        
        # Extract message type
        match = re.search(f'35=([^{self.SOH}]+)', msg)
        if not match:
            logger.warning("Invalid message format")
            return
            
        msg_type = match.group(1)
        
        if msg_type == '0':  # Heartbeat
            logger.debug("Received Heartbeat")
        elif msg_type == '1':  # Test Request
            # Extract TestReqID
            match = re.search(f'112=([^{self.SOH}]+)', msg)
            if match:
                test_req_id = match.group(1)
                self.send_heartbeat_response(test_req_id)
        elif msg_type == 'A':  # Logon
            logger.info("Logon successful")
        elif msg_type == '5':  # Logout
            logger.info("Received Logout")
            self.connected = False
        elif msg_type == 'W':  # Market Data Snapshot
            self.handle_market_data(msg)
        elif msg_type == 'Y':  # Market Data Request Reject
            self.handle_market_data_reject(msg)
    
    def send_heartbeat_response(self, test_req_id):
        """Send heartbeat response to a test request"""
        body = f"112={test_req_id}{self.SOH}"  # TestReqID
        msg = self.create_fix_message("0", body)
        self.send_message(msg)
    
    def handle_market_data(self, msg):
        """Process market data snapshot message"""
        # Extract symbol
        match = re.search(f'55=([^{self.SOH}]+)', msg)
        if not match:
            logger.warning("Symbol not found in market data message")
            return
            
        symbol = match.group(1)
        
        # Initialize data for this symbol if not exists
        if symbol not in self.market_data:
            self.market_data[symbol] = {'bids': [], 'asks': [], 'mid': None, 'timestamp': None}
            
        # Get current timestamp
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.market_data[symbol]['timestamp'] = current_time
        
        # Reset current bids/asks
        self.market_data[symbol]['bids'] = []
        self.market_data[symbol]['asks'] = []
        
        # Extract number of entries
        match = re.search(r'268=(\d+)', msg)
        if not match:
            logger.warning("NoMDEntries not found in market data message")
            return
            
        entries_count = int(match.group(1))
        
        # Extract and process entries
        entry_pattern = f'269=([^{self.SOH}]+){self.SOH}270=([^{self.SOH}]+)(?:{self.SOH}271=([^{self.SOH}]+))?'
        for entry_match in re.finditer(entry_pattern, msg):
            entry_type = entry_match.group(1)
            price = float(entry_match.group(2))
            
            # Size might be optional
            size = None
            if entry_match.group(3):
                size = float(entry_match.group(3))
            
            if entry_type == '0':  # Bid
                self.market_data[symbol]['bids'].append({'price': price, 'size': size})
            elif entry_type == '1':  # Offer
                self.market_data[symbol]['asks'].append({'price': price, 'size': size})
            elif entry_type == 'PM':  # Mid-Point
                self.market_data[symbol]['mid'] = price
                
        # Sort bids and asks
        self.market_data[symbol]['bids'].sort(key=lambda x: x['price'], reverse=True)
        self.market_data[symbol]['asks'].sort(key=lambda x: x['price'])
        
        # Display market data update
        self.display_market_data_update(symbol)
        
        # Save market data to CSV
        self.save_market_data_to_csv(symbol)
    
    def handle_market_data_reject(self, msg):
        """Handle market data request rejections"""
        # Extract reason
        match = re.search(r'281=(\d+)', msg)
        if match:
            reason_code = match.group(1)
            logger.warning(f"Market data request rejected with code: {reason_code}")
            
        # Extract text
        match = re.search(f'58=([^{self.SOH}]+)', msg)
        if match:
            text = match.group(1)
            logger.warning(f"Rejection reason: {text}")
    
    def display_market_data_update(self, symbol):
        """Display update for a specific symbol"""
        data = self.market_data[symbol]
        logger.info(f"Market data update for {symbol} at {data['timestamp']}")
        
        if data['bids']:
            top_bid = data['bids'][0]['price']
            logger.info(f"Top bid: {top_bid}")
            
        if data['asks']:
            top_ask = data['asks'][0]['price']
            logger.info(f"Top ask: {top_ask}")
            
        if data['mid'] is not None:
            logger.info(f"Mid price: {data['mid']}")
            
        if data['bids'] and data['asks']:
            spread = data['asks'][0]['price'] - data['bids'][0]['price']
            logger.info(f"Spread: {spread}")
    
    def save_market_data_to_csv(self, symbol):
        """Save market data for a symbol to CSV file"""
        data = self.market_data[symbol]
        timestamp = data['timestamp']
        
        # Create a directory for data if it doesn't exist
        os.makedirs('market_data', exist_ok=True)
        
        # Format timestamp for filename
        date_str = timestamp.split(' ')[0]
        
        # Create a dataframe for this update
        rows = []
        
        # Add bids
        for i, bid in enumerate(data['bids']):
            rows.append({
                'timestamp': timestamp,
                'symbol': symbol,
                'side': 'bid',
                'level': i+1,
                'price': bid['price'],
                'size': bid['size']
            })
            
        # Add asks
        for i, ask in enumerate(data['asks']):
            rows.append({
                'timestamp': timestamp,
                'symbol': symbol,
                'side': 'ask',
                'level': i+1,
                'price': ask['price'],
                'size': ask['size']
            })
            
        # Add mid price
        if data['mid'] is not None:
            rows.append({
                'timestamp': timestamp,
                'symbol': symbol,
                'side': 'mid',
                'level': 0,
                'price': data['mid'],
                'size': None
            })
            
        # Create dataframe
        df = pd.DataFrame(rows)
        
        # Save to CSV
        filename = f"market_data/{symbol.replace('/', '_')}_{date_str}.csv"
        file_exists = os.path.isfile(filename)
        
        # Append to file if it exists, otherwise create new file with header
        df.to_csv(filename, mode='a', header=not file_exists, index=False)
            
    def heartbeat_sender(self):
        """Send heartbeats periodically"""
        logger.info("Heartbeat sender thread started")
        while self.running and self.connected:
            time.sleep(25)  # Send heartbeat every 25 seconds (slightly less than the 30s interval)
            if self.connected:
                self.send_heartbeat()
        logger.info("Heartbeat sender thread stopped")

    def get_current_prices(self):
        """Get the current top of book prices for all symbols"""
        result = {}
        for symbol, data in self.market_data.items():
            result[symbol] = {
                'timestamp': data['timestamp'],
                'bid': data['bids'][0]['price'] if data['bids'] else None,
                'ask': data['asks'][0]['price'] if data['asks'] else None,
                'mid': data['mid']
            }
        return result

def main():
    """Main function to run the Bosonic client"""
    # Load connection settings from environment variables
    host = os.getenv("BOSONIC_HOST")
    port = os.getenv("BOSONIC_PORT")
    sender_comp_id = os.getenv("BOSONIC_SENDER_COMP_ID")
    target_comp_id = os.getenv("BOSONIC_TARGET_COMP_ID")
    
    # Get trading pairs from environment variable
    trading_pairs_str = os.getenv("TRADING_PAIRS", "EUR/USD,BTC/USD,ETH/USD")
    trading_pairs = [pair.strip() for pair in trading_pairs_str.split(",")]
    
    # Validate settings
    if not all([host, port, sender_comp_id, target_comp_id]):
        logger.error("Missing required environment variables. Check your .env file.")
        return
    
    # Create client
    client = BosionicClient(host, port, sender_comp_id, target_comp_id)
    
    try:
        # Connect to server
        if client.connect():
            logger.info(f"Successfully connected to Bosonic at {host}:{port}")
            
            # Request market data
            client.request_market_data(trading_pairs)
            
            # Run until user interrupts
            try:
                while client.connected:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("User interrupted. Shutting down...")
        else:
            logger.error("Failed to connect to server")
    
    except Exception as e:
        logger.error(f"Error: {e}")
    
    finally:
        # Ensure clean shutdown
        client.disconnect()

if __name__ == "__main__":
    main()