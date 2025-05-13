#!/usr/bin/env python3
import socket
import subprocess
import time

# Create a client that exactly matches your curl command
class BosonicClient:
    def __init__(self):
        self.host = '127.0.0.1'
        self.port = 18305
        self.socket = None
    
    def connect(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            print(f"Connected to {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"Connection error: {e}")
            return False
    
    def send_logon(self):
        # Use date command to match exactly what curl is doing
        date_cmd = subprocess.Popen(['date', '+%Y%m%d-%H:%M:%S'], 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE)
        stdout, _ = date_cmd.communicate()
        current_date = stdout.decode().strip()
        
        # Exact format from your curl command
        logon_msg = f"8=FIX.4.4\x0135=A\x0134=1\x0149=KUROSHIO_PRICE_FEED_MD\x0156=OGG_MD_KUROSHIO_PRICE_FEED\x0152={current_date}\x0198=0\x01108=30\x0110=000\x01"
        
        print("Sending logon message:")
        print(repr(logon_msg))
        try:
            self.socket.sendall(logon_msg.encode())
            print("Logon message sent")
            return True
        except Exception as e:
            print(f"Error sending logon: {e}")
            return False
    
    def receive(self):
        try:
            self.socket.settimeout(10)
            data = self.socket.recv(4096)
            if data:
                print(f"Received data: {data}")
            else:
                print("No data received")
            return data
        except socket.timeout:
            print("Socket timeout waiting for response")
            return None
        except Exception as e:
            print(f"Error receiving data: {e}")
            return None
    
    def close(self):
        if self.socket:
            self.socket.close()
            print("Connection closed")

# Main function
def main():
    client = BosonicClient()
    
    if client.connect():
        if client.send_logon():
            response = client.receive()
            if response:
                print("Successfully received response")
            else:
                print("No response received")
        client.close()

if __name__ == "__main__":
    main()
