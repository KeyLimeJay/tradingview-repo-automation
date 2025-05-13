#!/usr/bin/env python3
import socket
import time
import datetime

class BosonicClient:
    def __init__(self, host='127.0.0.1', port=18305, 
                 sender_comp_id='KUROSHIO_PRICE_FEED_MD', 
                 target_comp_id='OGG_MD_KUROSHIO_PRICE_FEED',
                 heartbeat_interval=30):
        """
        Initialize Bosonic FIX client with connection parameters
        """
        self.host = host
        self.port = port
        self.sender_comp_id = sender_comp_id
        self.target_comp_id = target_comp_id
        self.heartbeat_interval = heartbeat_interval
        self.socket = None
        self.seq_num = 1
        self.session_established = False
        self.trading_pairs = ["BTC/USD", "ETH/USD"]  # Default trading pairs from .env file
    
    def connect(self):
        """
        Establish socket connection to Bosonic FIX server
        """
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            print(f"Connected to {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False
            
    def calculate_checksum(self, message):
        """
        Calculate FIX checksum (sum of all bytes modulo 256)
        """
        # Remove SOH character (ASCII 1) for checksum calculation 
        checksum = sum(message.replace('\x01', '').encode('utf-8')) % 256
        return f"{checksum:03d}"
    
    def create_message(self, msg_type, fields):
        """
        Create a FIX message with proper header, body and trailer
        """
        # Standard header fields
        header = [
            f"8=FIX.4.4",  # BeginString
            f"9=0",  # BodyLength placeholder
            f"35={msg_type}",  # MsgType
            f"49={self.sender_comp_id}",  # SenderCompID
            f"56={self.target_comp_id}",  # TargetCompID
            f"34={self.seq_num}",  # MsgSeqNum
            f"52={datetime.datetime.utcnow().strftime('%Y%m%d-%H:%M:%S')}"  # SendingTime
        ]
        
        # Combine header and body
        message = header + fields
        
        # Join with SOH delimiter
        msg_str = "\x01".join(message) + "\x01"
        
        # Calculate body length (exclude tags 8, 9, and the delimiters between them)
        body_len = len(msg_str) - msg_str.find("\x0135=") + 2
        
        # Update body length
        msg_str = msg_str.replace("9=0", f"9={body_len}")
        
        # Calculate and append checksum
        checksum = self.calculate_checksum(msg_str)
        msg_str += f"10={checksum}\x01"
        
        return msg_str
    
    def send_message(self, message):
        """
        Send a message to the FIX server
        """
        try:
            self.socket.sendall(message.encode('utf-8'))
            self.seq_num += 1
            return True
        except Exception as e:
            print(f"Error sending message: {e}")
            return False
    
    def receive_message(self, timeout=5):
        """
        Receive and parse a FIX message from the server
        """
        try:
            self.socket.settimeout(timeout)
            buffer = b""
            while b"\x0110=" not in buffer:
                data = self.socket.recv(4096)
                if not data:
                    break
                buffer += data
            
            # Process received messages
            messages = buffer.split(b"\x0110=")
            parsed_messages = []
            
            for msg in messages:
                if not msg:
                    continue
                # Add back the checksum part that was removed in the split
                if msg != messages[-1]:
                    msg = msg + b"\x0110=" + messages[messages.index(msg) + 1].split(b"\x01")[0]
                
                # Decode and parse the message
                try:
                    decoded = msg.decode('utf-8')
                    msg_parts = decoded.split("\x01")
                    msg_dict = {}
                    
                    for part in msg_parts:
                        if "=" in part:
                            tag, value = part.split("=", 1)
                            msg_dict[tag] = value
                    
                    parsed_messages.append(msg_dict)
                except Exception as e:
                    print(f"Error parsing message: {e}")
            
            return parsed_messages
        except socket.timeout:
            print("Socket timeout waiting for response")
            return []
        except Exception as e:
            print(f"Error receiving message: {e}")
            return []
    
    def logon(self):
        """
        Send a Logon message to establish FIX session
        """
        logon_fields = [
            f"98=0",  # EncryptMethod (none)
            f"108={self.heartbeat_interval}"  # HeartBeat interval
        ]
        
        logon_msg = self.create_message("A", logon_fields)
        if self.send_message(logon_msg):
            print("Logon message sent")
            
            responses = self.receive_message()
            for response in responses:
                if response.get("35") == "A":
                    self.session_established = True
                    print("Logon successful, session established")
                    return True
            
            print("Did not receive logon confirmation")
            return False
        else:
            print("Failed to send logon message")
            return False
    
    def heartbeat(self):
        """
        Send a heartbeat message
        """
        heartbeat_msg = self.create_message("0", [])
        if self.send_message(heartbeat_msg):
            print("Heartbeat sent")
            return True
        else:
            print("Failed to send heartbeat")
            return False
    
    def request_market_data(self, symbols=None):
        """
        Request market data for specific trading pairs
        """
        if not symbols:
            symbols = self.trading_pairs
        
        # Convert symbols to Bosonic format (Base/Local)
        formatted_symbols = []
        for symbol in symbols:
            if "/" in symbol:
                formatted_symbols.append(symbol.replace("/", ""))
        
        md_req_id = f"MD_{int(time.time())}"  # Create unique MD request ID
        
        md_fields = [
            f"262={md_req_id}",  # MDReqID
            f"263=1",  # SubscriptionRequestType (1=Subscribe)
            f"264=0",  # MarketDepth (0=Full book)
            f"265=0",  # MDUpdateType (0=Full refresh)
            f"267=2"  # NoMDEntryTypes (2 types: bid and offer)
        ]
        
        # Add entry types
        md_fields.append("269=0")  # MDEntryType (0=Bid)
        md_fields.append("269=1")  # MDEntryType (1=Offer)
        
        # Add symbols
        md_fields.append(f"146={len(formatted_symbols)}")  # NoRelatedSym
        
        for symbol in formatted_symbols:
            md_fields.append(f"55={symbol}")  # Symbol
        
        md_request_msg = self.create_message("V", md_fields)
        if self.send_message(md_request_msg):
            print(f"Market data request sent for {', '.join(symbols)}")
            
            # Wait for and process market data response
            responses = self.receive_message(10)  # Longer timeout for market data
            market_data = []
            
            for response in responses:
                # Check if it's a market data message
                if response.get("35") == "W":  # Market Data Snapshot Full Refresh
                    print("Received market data snapshot")
                    market_data.append(response)
                elif response.get("35") == "Y":  # Market Data Request Reject
                    print(f"Market data request rejected: {response.get('58', 'No reason provided')}")
            
            return market_data
        else:
            print("Failed to send market data request")
            return []
    
    def logout(self):
        """
        Send a Logout message and close the connection
        """
        if self.session_established:
            logout_msg = self.create_message("5", [])
            if self.send_message(logout_msg):
                print("Logout message sent")
                
                responses = self.receive_message()
                for response in responses:
                    if response.get("35") == "5":
                        print("Logout acknowledged")
                        break
            else:
                print("Failed to send logout message")
        
        if self.socket:
            self.socket.close()
            print("Connection closed")
        
        self.session_established = False
    
    def format_market_data(self, market_data):
        """
        Format market data into readable output
        """
        if not market_data:
            return "No market data received"
        
        output = []
        
        for msg in market_data:
            symbol = msg.get("55", "Unknown")
            entry_count = int(msg.get("268", "0"))
            
            output.append(f"Market data for {symbol}:")
            
            # Parse entries
            entries = []
            for i in range(entry_count):
                entry_type = msg.get(f"269{i}")
                price = msg.get(f"270{i}")
                size = msg.get(f"271{i}")
                
                if entry_type == "0":
                    entry_type = "Bid"
                elif entry_type == "1":
                    entry_type = "Offer"
                elif entry_type == "PB":
                    entry_type = "Peg Bid"
                elif entry_type == "PO":
                    entry_type = "Peg Offer"
                elif entry_type == "PM":
                    entry_type = "Peg Mid"
                
                if price and entry_type:
                    entry = f"{entry_type}: {price}"
                    if size:
                        entry += f" (Size: {size})"
                    entries.append(entry)
            
            output.append("\n".join(entries))
            output.append("")
        
        return "\n".join(output)

def main():
    """
    Main function to execute the Bosonic client
    """
    # Create and connect client
    client = BosonicClient()
    
    if client.connect():
        try:
            # Establish FIX session
            if client.logon():
                print("Successfully logged on to Bosonic FIX API")
                
                # Request market data for BTC/USD and ETH/USD
                market_data = client.request_market_data()
                
                # Format and display the data
                formatted_data = client.format_market_data(market_data)
                print("\nMARKET DATA:")
                print(formatted_data)
                
                # Send heartbeat
                client.heartbeat()
                
                # Wait a moment to see if we get any more data
                time.sleep(2)
                
            else:
                print("Failed to establish FIX session")
        finally:
            # Always properly logout and close connection
            client.logout()
    else:
        print("Failed to connect to Bosonic server")

if __name__ == "__main__":
    main()