#rest.py
import requests # type: ignore
import sys
import os

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from config.settings import API_URL

def get_xrp_pairs():
    """
    Fetch all trading pairs from Bitstamp that involve XRP.
    Returns a list of market symbols for XRP pairs and a list of all unique assets.
    """
    try:
        print("Fetching XRP trading pairs from Bitstamp API...")
        response = requests.get(f"{API_URL}/markets/")
        
        if response.status_code == 200:
            pairs_data = response.json()
            xrp_pairs = []
            all_assets = set()
            
            # Identify XRP pairs and collect unique assets
            for pair in pairs_data:
                if "XRP" in pair["name"]:
                    xrp_pairs.append(pair["market_symbol"])
                    # Add base and counter currencies to the set of all assets
                    all_assets.add(pair["base_currency"])
                    all_assets.add(pair["counter_currency"])
            
            # Remove any duplicate pairs
            xrp_pairs = list(set(xrp_pairs))
            
            if not xrp_pairs:
                raise Exception("No XRP pairs found in API response")
            
            print(f"Found {len(xrp_pairs)} XRP trading pairs: {', '.join(xrp_pairs)}")
            return xrp_pairs, list(all_assets)
        else:
            raise Exception(f"API returned status code {response.status_code}")
    
    except Exception as e:
        print(f"Error fetching XRP trading pairs: {e}")
        print("Retrying in 5 seconds...")
        import time
        time.sleep(5)
        return get_xrp_pairs()

def get_usd_pairs(assets):
    """
    Fetch all USD trading pairs for the assets we're interested in.
    This will help us normalize values to USD.
    """
    try:
        print("Fetching USD pairs for all assets...")
        response = requests.get(f"{API_URL}/markets/")
        
        if response.status_code == 200:
            pairs_data = response.json()
            usd_pairs = []
            
            # Find all USD pairs for our assets
            for pair in pairs_data:
                base = pair["base_currency"]
                counter = pair["counter_currency"]
                
                # Direct USD pairs
                if counter == "USD" and base in assets:
                    usd_pairs.append(pair["market_symbol"])
                # Also include EUR/USD pair for indirect conversions
                elif base == "EUR" and counter == "USD":
                    usd_pairs.append(pair["market_symbol"])
            
            print(f"Found {len(usd_pairs)} USD pairs for normalization: {', '.join(usd_pairs)}")
            return usd_pairs
        else:
            raise Exception(f"API returned status code {response.status_code}")
    
    except Exception as e:
        print(f"Error fetching USD pairs: {e}")
        print("Retrying in 5 seconds...")
        import time
        time.sleep(5)
        return get_usd_pairs(assets)

def fetch_order_book(pair):
    """
    Fetch the current order book for a specific pair.
    """
    try:
        response = requests.get(f"{API_URL}/order_book/{pair}/")
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch order book for {pair}: HTTP {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching order book for {pair}: {e}")
        return None