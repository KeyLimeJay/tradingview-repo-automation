Multi-Account Trading Bot

A robust cryptocurrency trading bot that supports multiple trading accounts, timeframe-based routing, and automatic position management.

## Features

- **Multi-Account Support**: Route trading signals to different accounts based on timeframes
- **Configuration-Driven**: All settings configurable via JSON files
- **Position Management**: Automatic position tracking and limit enforcement
- **Auto-Short Capability**: Automatically shorts positions exceeding limits
- **Repo Integration**: Handles crypto repo operations (borrowing/lending)
- **Robust Error Handling**: Comprehensive retry logic and failure recovery
- **Webhook API**: Simple webhook API for receiving trading signals

## Architecture

The application is structured as follows:
trading_bot_v2/
├── config/                   # Configuration files
│   ├── config.json           # Main configuration
│   └── credentials/          # Account credentials
│       ├── default.json
│       ├── tm5.json
│       └── tm6.json
├── logs/                     # Log files
├── src/                      # Source code
│   ├── account_manager.py    # Account management
│   ├── auto_position_manager.py # Position management
│   ├── config_manager.py     # Configuration handling
│   ├── position_websocket.py # Position tracking
│   ├── trading_bot.py        # Trading logic
│   └── trading_utils.py      # Trading utilities
├── main.py                   # Entry point
└── requirements.txt          # Dependencies
Copy
## Installation

1. Clone the repository:
git clone https://github.com/yourusername/trading_bot_v2.git
cd trading_bot_v2
Copy
2. Create a virtual environment and install dependencies:
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
Copy
3. Configure the application:
- Update the account credentials in `config/credentials/`
- Customize settings in `config/config.json`

## Running the Bot

Start the bot with:
python main.py
Copy
For custom configuration file:
python main.py --config /path/to/config.json
Copy
## Configuration

### Main Configuration (config.json)

The main configuration file defines global settings, account configurations, and trading parameters:

```json
{
  "version": "2.0.0",
  "global": {
    "port": 6101,
    "host": "0.0.0.0",
    "environment": "production",
    "log_level": "INFO",
    ...
  },
  "accounts": [
    {
      "name": "default",
      "credentials_file": "credentials/default.json",
      "timeframes": ["1h", "4h", "1d"],
      "trading_pairs": ["BTC/USDC", "ETH/USDC"],
      "enabled": true,
      "trading": {
        ...
      },
      "currencies": {
        ...
      }
    },
    ...
  ]
}
Account Credentials (credentials/*.json)
Each account has its own credentials file with API keys and endpoints:
jsonCopy{
  "api_key": "your-api-key",
  "api_secret": "your-api-secret",
  "api_username": "username",
  "api_password": "password",
  "api_code": "auth-code",
  "api_url": "https://api.example.com",
  "api_base_url": "https://trading.example.com",
  "ws_url": "wss://trading.example.com/websocket",
  "custodian_id": "CUSTODIAN_NAME"
}
API Endpoints

POST /webhook: Receive trading signals
GET /positions: Get current positions for all accounts
GET /health: Check system health
POST /auto-short: Manually trigger auto-short

License
MIT License