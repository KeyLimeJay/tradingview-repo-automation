#!/usr/bin/env python3
import subprocess
import time

def run_curl_command():
    cmd = 'echo -ne "8=FIX.4.4\\x0135=A\\x0134=1\\x0149=KUROSHIO_PRICE_FEED_MD\\x0156=OGG_MD_KUROSHIO_PRICE_FEED\\x0152=$(date +%Y%m%d-%H:%M:%S)\\x0198=0\\x01108=30\\x0110=000\\x01" | curl -v --data-binary @- telnet://127.0.0.1:18305'
    
    print(f"Running command: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    print("STDOUT:")
    print(result.stdout)
    
    print("STDERR:")
    print(result.stderr)
    
    return result.returncode

if __name__ == "__main__":
    run_curl_command()
