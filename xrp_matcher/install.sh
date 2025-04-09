#!/bin/bash

echo "==== XRP Matcher Installation ===="

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install required packages
pip install websocket-client python-dotenv requests websockets

echo "Installation complete!"
echo "Use ./start_xrp_matcher.sh to start the application"
