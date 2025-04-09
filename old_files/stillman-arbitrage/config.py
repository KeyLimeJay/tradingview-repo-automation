# Stillman API Configuration
STILLMAN_CONFIG = {
    "publicKey": "FBUKA51HWPDK9WGEXMFRBU7E",
    "privateKey": "IYZ5R3K622K9D9IQTHCL2XUV39LPTU8395FVSZEXI7SVRO62",   
    "wsUrl": "wss://sandbox-api.stillmandigital.com/v1/stream",  # Sandbox URL
    "pair": "XRP/USD",
    "rfqType": "ONE_SECOND",  # Options: ONE_SECOND, FIVE_SECONDS, THIRTY_SECONDS
    "levels": [0.5, 1, 5, 10, 50]  # Quantity levels you're interested in
}

# Your discount source XRP price
DISCOUNT_SOURCE_PRICE = 0.59  # Example price in USD (update with your actual price)

# Minimum profit threshold to consider a trade worthwhile
MIN_PROFIT_THRESHOLD = 0.02  # 2%