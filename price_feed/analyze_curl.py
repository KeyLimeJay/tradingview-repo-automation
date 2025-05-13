#!/usr/bin/env python3
import os
import tempfile
import subprocess
import time

# Create a temporary file to capture tcpdump output
with tempfile.NamedTemporaryFile(delete=False) as tmp:
    tmp_name = tmp.name

print(f"Starting packet capture to {tmp_name}")

# Start tcpdump in the background
try:
    dump_cmd = f"tcpdump -i lo -w {tmp_name} port 18305 -s 0"
    print(f"Running: {dump_cmd}")
    tcpdump_proc = subprocess.Popen(dump_cmd, shell=True)
    
    # Wait for tcpdump to start
    time.sleep(2)
    
    # Now run the curl command
    curl_cmd = 'echo -ne "8=FIX.4.4\\x0135=A\\x0134=1\\x0149=KUROSHIO_PRICE_FEED_MD\\x0156=OGG_MD_KUROSHIO_PRICE_FEED\\x0152=$(date +%Y%m%d-%H:%M:%S)\\x0198=0\\x01108=30\\x0110=000\\x01" | curl -v --data-binary @- telnet://127.0.0.1:18305'
    print("\nRunning curl command:")
    print(curl_cmd)
    curl_proc = subprocess.Popen(curl_cmd, shell=True)
    
    # Wait for curl to do its thing
    time.sleep(5)
    
    # Kill the processes
    curl_proc.terminate()
    tcpdump_proc.terminate()
    
    # Read the captured packets
    print("\nAnalyzing captured packets:")
    analyze_cmd = f"tcpdump -r {tmp_name} -X"
    os.system(analyze_cmd)
    
    # Clean up
    os.unlink(tmp_name)
    
except Exception as e:
    print(f"Error: {e}")
    # Try to clean up
    try:
        tcpdump_proc.terminate()
    except:
        pass
    try:
        curl_proc.terminate()
    except:
        pass
    try:
        os.unlink(tmp_name)
    except:
        pass
