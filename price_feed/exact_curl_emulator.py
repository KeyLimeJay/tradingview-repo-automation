#!/usr/bin/env python3
import os
import socket
import time

# First run the actual curl command that works
print("Running the original curl command...")
os.system('echo -ne "8=FIX.4.4\x0135=A\x0134=1\x0149=KUROSHIO_PRICE_FEED_MD\x0156=OGG_MD_KUROSHIO_PRICE_FEED\x0152=$(date +%Y%m%d-%H:%M:%S)\x0198=0\x01108=30\x0110=000\x01" | curl -v --data-binary @- telnet://127.0.0.1:18305')

# Wait a moment
time.sleep(3)

# Now try with raw socket, creating an exact binary copy of the curl data
print("\nNow trying with raw Python socket...")

# Get the exact same data as the curl command
from subprocess import Popen, PIPE
date_cmd = Popen(['date', '+%Y%m%d-%H:%M:%S'], stdout=PIPE)
date_output, _ = date_cmd.communicate()
current_date = date_output.strip()

# Create the message using actual bytes, not string encoding
logon_msg = b"8=FIX.4.4\x0135=A\x0134=1\x0149=KUROSHIO_PRICE_FEED_MD\x0156=OGG_MD_KUROSHIO_PRICE_FEED\x0152=" + current_date + b"\x0198=0\x01108=30\x0110=000\x01"

print(f"Sending raw message: {repr(logon_msg)}")

# Create socket and send
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('127.0.0.1', 18305))
sock.sendall(logon_msg)

# Try to receive with a reasonable timeout
sock.settimeout(5)
try:
    data = sock.recv(4096)
    print(f"Received: {repr(data)}")
except socket.timeout:
    print("Socket timeout - no response received")
except Exception as e:
    print(f"Error: {e}")
finally:
    sock.close()
