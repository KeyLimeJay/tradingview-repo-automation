#!/usr/bin/env python3
import socket
import subprocess
import binascii

def calculate_fix_checksum(message):
    """Calculate the proper FIX checksum"""
    # Sum ASCII values of all characters, mod 256
    checksum = 0
    for char in message:
        checksum += char
    checksum = checksum % 256
    return f"{checksum:03d}"  # Format as 3-digit string

def run_socket_test():
    print("\n=== RUNNING PYTHON SOCKET TEST WITH PROPER CHECKSUM ===")
    
    # Get current date in correct format
    date_proc = subprocess.Popen(['date', '+%Y%m%d-%H:%M:%S'], stdout=subprocess.PIPE)
    date_output, _ = date_proc.communicate()
    date_str = date_output.strip()
    
    # Create the message without checksum
    msg_without_checksum = b"8=FIX.4.4\x0135=A\x0134=1\x0149=KUROSHIO_PRICE_FEED_MD\x0156=OGG_MD_KUROSHIO_PRICE_FEED\x0152=" + date_str + b"\x0198=0\x01108=30\x01"
    
    # Calculate checksum
    checksum = calculate_fix_checksum(msg_without_checksum)
    
    # Add checksum to message
    msg = msg_without_checksum + f"10={checksum}\x01".encode()
    
    # Show exactly what we're sending
    print(f"Sending message with calculated checksum: {repr(msg)}")
    print(f"Message as hex: {binascii.hexlify(msg)}")
    
    # Connect and send
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('127.0.0.1', 18305))
        print("Connected to 127.0.0.1:18305")
        
        # Send the message
        sock.sendall(msg)
        print("Message sent")
        
        # Set to non-blocking mode to check for any data
        sock.setblocking(False)
        
        # Try to receive multiple times
        print("Checking for response...")
        for _ in range(5):
            try:
                data = sock.recv(4096)
                if data:
                    print(f"Received data: {repr(data)}")
                    print(f"Data as hex: {binascii.hexlify(data)}")
                    break
                else:
                    print("Empty response")
            except BlockingIOError:
                print("No data available yet...")
            except Exception as e:
                print(f"Error receiving: {e}")
            
            # Wait a bit before checking again
            import time
            time.sleep(1)
        
    except Exception as e:
        print(f"Connection error: {e}")
    finally:
        sock.close()
        print("Socket closed")

# Run the test
run_socket_test()

print("\nTry running the original curl command in a separate terminal with:")
print('echo -ne "8=FIX.4.4\\x0135=A\\x0134=1\\x0149=KUROSHIO_PRICE_FEED_MD\\x0156=OGG_MD_KUROSHIO_PRICE_FEED\\x0152=$(date +%Y%m%d-%H:%M:%S)\\x0198=0\\x01108=30\\x0110=000\\x01" | strace -f curl -v --data-binary @- telnet://127.0.0.1:18305')
print("\nThis will show exactly what curl is doing at the system call level.")
