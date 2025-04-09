

This script monitors XRP prices on Stillman's platform and compares them with a discount source to identify arbitrage opportunities.

## Setup

1. Update `config.py` with your actual Stillman API credentials
2. Install requirements:
pip install -r requirements.txt
Copy3. Run the script:
python xrp_price_monitor.py
Copy
## Features

- Real-time monitoring of XRP/USD prices via Stillman's RFQ stream
- Configurable quantity levels for price checking
- Automatic arbitrage opportunity detection
- Detailed logging to both console and file

## Configuration

Edit `config.py` to:
- Update your API credentials
- Change the trading pair
- Adjust your discount source price
- Modify the minimum profit threshold
- Change quantity levels of interest