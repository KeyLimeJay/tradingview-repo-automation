#!/usr/bin/env python3
import socket
import time
import os

def run_test():
    # Create raw socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        # Connect
        print(f"Connecting to 127.0.0.1:18305...")
        sock.connect(('127.0.0.1', 18305))
        print("Connected")
        
        # Get raw FIX message from curl (ensure exact same message)
        cmd = 'echo -ne "8=FIX.4.4\x0135=A\x0134=1\x0149=KUROSHIO_PRICE_FEED_MD\x0156=OGG_MD_KUROSHIO_PRICE_FEED\x0152=$(date +%Y%m%d-%H:%M:%S)\x0198=0\x01108=30\x0110=000\x01"'
        raw_msg = os.popen(cmd).read().encode()
        
        print(f"Sending raw message: {repr(raw_msg)}")
        sock.sendall(raw_msg)
        print("Message sent")
        
        # Wait to see if any data comes back
        sock.settimeout(5)
        try:
            data = sock.recv(4096)
            print(f"Received: {repr(data)}")
        except socket.timeout:
            print("Timeout - no response")
    
    except Exception as e:
        print(f"Error: {e}")
    finally:
        sock.close()
        print("Socket closed")

run_test()
