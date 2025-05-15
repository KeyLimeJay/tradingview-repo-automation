#!/usr/bin/env python3
"""
Bosonic Price Feed Client - With Source Information
Captures and displays information about the liquidity provider or source of each price
"""

import quickfix as fix
import logging
import time
import sys
import argparse
import signal
import os
import json
import threading

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("price_feed.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('BosonicPriceFeed')

class PriceFeedApplication(fix.Application):
    def __init__(self, subscribe_all=True, symbols=None):
        super().__init__()
        self.session_id = None
        self.logged_on = False
        self.market_data = {}  # Store the latest market data for each symbol
        self.running = True
        self.subscribe_all = subscribe_all
        self.specified_symbols = symbols if symbols else []
        self.discovered_symbols = set()
        self.subscribed_symbols = set()
        self.valid_symbols = set()  # Symbols with valid data
        
        # Focus on major pairs first
        self.common_pairs = [
            "EUR/USD", "USD/JPY", "GBP/USD", "USD/CHF", "USD/CAD", 
            "AUD/USD", "NZD/USD", "EUR/GBP", "EUR/JPY", "BTC/USD",
            "ETH/USD", "BTC/USDT", "ETH/USDT"
        ]

    def onCreate(self, sessionID):
        logger.info(f"Session created: {sessionID}")
        self.session_id = sessionID
        return

    def onLogon(self, sessionID):
        logger.info(f"Logged on: {sessionID}")
        self.logged_on = True
        
        # If specific symbols are provided, subscribe to those
        if self.specified_symbols:
            for symbol in self.specified_symbols:
                self.subscribe_market_data(symbol)
                self.subscribed_symbols.add(symbol)
        # If subscribing to all, try with common pairs first
        elif self.subscribe_all:
            logger.info("Subscribing to common currency pairs...")
            for symbol in self.common_pairs:
                if self.subscribe_market_data(symbol):
                    self.subscribed_symbols.add(symbol)
        
        # Schedule a periodic check for new pairs (if subscribe_all is True)
        if self.subscribe_all:
            self.schedule_discovery()
            
        return

    def onLogout(self, sessionID):
        logger.info(f"Logged out: {sessionID}")
        self.logged_on = False
        return

    def toAdmin(self, message, sessionID):
        msgType = fix.MsgType()
        message.getHeader().getField(msgType)
        
        if msgType.getValue() == fix.MsgType_Logon:
            logger.info("Sending Logon message")
        
        logger.debug(f"Sending admin message: {message}")
        return

    def fromAdmin(self, message, sessionID):
        logger.debug(f"Received admin message: {message}")
        return

    def toApp(self, message, sessionID):
        logger.debug(f"Sending application message: {message}")
        return

    def fromApp(self, message, sessionID):
        msgType = fix.MsgType()
        message.getHeader().getField(msgType)
        
        if msgType.getValue() == fix.MsgType_MarketDataSnapshotFullRefresh:
            self.process_market_data(message)
        elif msgType.getValue() == "Y":  # MarketDataRequestReject
            self.process_market_data_reject(message)
        return

    def process_market_data_reject(self, message):
        """Process market data rejection messages"""
        try:
            # Extract MDReqID to identify which symbol was rejected
            md_req_id = fix.MDReqID()
            if message.isSetField(md_req_id):
                message.getField(md_req_id)
                req_id = md_req_id.getValue()
                
                # Extract the symbol from the request ID (if our format is used)
                if req_id.startswith("MDR-"):
                    parts = req_id.split("-")
                    if len(parts) >= 3:
                        symbol = parts[1]
                        logger.warning(f"Market data request rejected for symbol: {symbol}")
                        
                        # Remove from subscribed symbols
                        if symbol in self.subscribed_symbols:
                            self.subscribed_symbols.remove(symbol)
                        if symbol in self.valid_symbols:
                            self.valid_symbols.remove(symbol)
            
            # Get reason text if available
            text = fix.Text()
            if message.isSetField(text):
                message.getField(text)
                reason = text.getValue()
                logger.warning(f"Rejection reason: {reason}")
                
        except Exception as e:
            logger.error(f"Error processing market data reject: {e}")

    def extract_source_info(self, message, group, symbol_value):
        """Extract source/liquidity provider information from the message"""
        source_info = {}
        
        # Check for MDEntryOriginator (432) - Trading desk or institution
        if group.isSetField(432):
            originator = fix.MDEntryOriginator()
            group.getField(originator)
            source_info["originator"] = originator.getValue()
        
        # Check for QuoteOriginationType (1214)
        if group.isSetField(1214):
            origin_type = fix.QuoteOriginationType()
            group.getField(origin_type)
            origin_value = origin_type.getValue()
            if origin_value == '0':
                source_info["origin_type"] = "Customer"
            elif origin_value == '1':
                source_info["origin_type"] = "Market Maker"
            elif origin_value == '2':
                source_info["origin_type"] = "System"
            else:
                source_info["origin_type"] = origin_value
        
        # Check for MDOriginType (1024)
        if group.isSetField(1024):
            md_origin = fix.MDOriginType()
            group.getField(md_origin)
            origin_value = md_origin.getValue()
            if origin_value == '0':
                source_info["md_origin"] = "Book"
            elif origin_value == '1':
                source_info["md_origin"] = "Off Book"
            elif origin_value == '2':
                source_info["md_origin"] = "Cross"
            else:
                source_info["md_origin"] = origin_value
        
        # Check for any Party information
        if message.hasGroup(453):  # NoPartyIDs
            party_count = fix.NoPartyIDs()
            message.getField(party_count)
            
            for i in range(party_count.getValue()):
                party_group = fix.Group(453, 448)
                message.getGroup(i+1, party_group)
                
                party_id = fix.PartyID()
                if party_group.isSetField(party_id):
                    party_group.getField(party_id)
                    
                    party_role = fix.PartyRole()
                    if party_group.isSetField(party_role):
                        party_group.getField(party_role)
                        role_value = party_role.getValue()
                        
                        if role_value == '1':  # Executing Broker
                            source_info["executing_broker"] = party_id.getValue()
                        elif role_value == '2':  # Clearing Firm
                            source_info["clearing_firm"] = party_id.getValue()
                        elif role_value == '4':  # Introducing Broker
                            source_info["introducing_broker"] = party_id.getValue()
                        elif role_value == '12':  # Executing System
                            source_info["executing_system"] = party_id.getValue()
                        elif role_value == '13':  # Market Maker
                            source_info["market_maker"] = party_id.getValue()
                        elif role_value == '17':  # Contra Firm
                            source_info["contra_firm"] = party_id.getValue()
                        elif role_value == '36':  # Entering Firm
                            source_info["entering_firm"] = party_id.getValue()
                        elif role_value == '37':  # Entering Trader
                            source_info["entering_trader"] = party_id.getValue()
                        elif role_value == '38':  # Service Provider
                            source_info["service_provider"] = party_id.getValue()
        
        # Check for Source (Exchange) field
        if group.isSetField(207):  # SecurityExchange
            exchange = fix.SecurityExchange()
            group.getField(exchange)
            source_info["exchange"] = exchange.getValue()
        
        # Check for the desk field
        if group.isSetField(290):  # MDEntryPositionNo
            desk_no = fix.MDEntryPositionNo()
            group.getField(desk_no)
            source_info["desk_no"] = desk_no.getValue()
        
        # If we found any source info
        if source_info:
            # Add a human-readable source string
            source_list = []
            
            if "market_maker" in source_info:
                source_list.append(f"MM:{source_info['market_maker']}")
            if "originator" in source_info:
                source_list.append(f"Orig:{source_info['originator']}")
            if "executing_system" in source_info:
                source_list.append(f"Sys:{source_info['executing_system']}")
            if "exchange" in source_info:
                source_list.append(f"Ex:{source_info['exchange']}")
            if "service_provider" in source_info:
                source_list.append(f"SP:{source_info['service_provider']}")
                
            # If we have a source string, use it
            if source_list:
                source_info["source"] = " | ".join(source_list)
            else:
                # If we have source info but no main fields, show all fields
                source_info["source"] = str(source_info)
        
        return source_info

    def process_market_data(self, message):
        """Process and store market data messages"""
        try:
            # Extract symbol
            symbol = fix.Symbol()
            if message.isSetField(symbol):
                message.getField(symbol)
                symbol_value = symbol.getValue()
                
                # Add to discovered symbols
                self.discovered_symbols.add(symbol_value)
            else:
                logger.warning("Market data message missing Symbol field")
                return
            
            # Number of MD entries
            no_md_entries = fix.NoMDEntries()
            if not message.isSetField(no_md_entries):
                logger.warning(f"Market data message for {symbol_value} missing NoMDEntries field")
                logger.warning(f"Message: {message}") 
                return
                
            message.getField(no_md_entries)
            entry_count = no_md_entries.getValue()
            
            # Skip if no entries
            if entry_count == 0:
                logger.warning(f"Market data for {symbol_value} has 0 entries")
                return
            
            # Initialize data for this symbol if needed
            if symbol_value not in self.market_data:
                self.market_data[symbol_value] = {
                    'bid': None,
                    'ask': None,
                    'mid': None,
                    'bid_source': None,
                    'ask_source': None,
                    'bid_size': None,
                    'ask_size': None,
                    'timestamp': None
                }
            
            # Process each MD entry
            bid_found = False
            ask_found = False
            source_found = False
            
            for i in range(entry_count):
                group = fix.Group(268, 269)
                message.getGroup(i+1, group)
                
                # Entry type (0 = Bid, 1 = Offer)
                md_entry_type = fix.MDEntryType()
                group.getField(md_entry_type)
                entry_type_value = md_entry_type.getValue()
                
                # Extract source information from the entry
                source_info = self.extract_source_info(message, group, symbol_value)
                if source_info:
                    source_found = True
                
                # Price
                if group.isSetField(270):  # MDEntryPx
                    md_entry_px = fix.MDEntryPx()
                    group.getField(md_entry_px)
                    entry_price = md_entry_px.getValue()
                    
                    # Size/Quantity if available
                    entry_size = None
                    if group.isSetField(271):  # MDEntrySize
                        md_entry_size = fix.MDEntrySize()
                        group.getField(md_entry_size)
                        entry_size = md_entry_size.getValue()
                    
                    if entry_type_value == '0':  # Bid
                        self.market_data[symbol_value]['bid'] = entry_price
                        self.market_data[symbol_value]['bid_size'] = entry_size
                        if source_info and 'source' in source_info:
                            self.market_data[symbol_value]['bid_source'] = source_info['source']
                        entry_type = "Bid"
                        bid_found = True
                    elif entry_type_value == '1':  # Offer/Ask
                        self.market_data[symbol_value]['ask'] = entry_price
                        self.market_data[symbol_value]['ask_size'] = entry_size
                        if source_info and 'source' in source_info:
                            self.market_data[symbol_value]['ask_source'] = source_info['source']
                        entry_type = "Ask"
                        ask_found = True
                    elif entry_type_value == 'PM' or entry_type_value == '2':  # Mid price
                        self.market_data[symbol_value]['mid'] = entry_price
                        entry_type = "Mid"
                    else:
                        entry_type = entry_type_value
                    
                    source_str = f" [Source: {source_info.get('source', 'Unknown')}]" if source_info and 'source' in source_info else ""
                    logger.debug(f"{symbol_value} {entry_type}: {entry_price}{source_str}")
                else:
                    logger.warning(f"MD entry {i+1} for {symbol_value} missing price")
            
            # Only proceed if we have both bid and ask
            if not (bid_found and ask_found):
                logger.warning(f"Market data for {symbol_value} incomplete - bid: {bid_found}, ask: {ask_found}")
                return
                
            # Add to valid symbols
            self.valid_symbols.add(symbol_value)
            
            # Calculate mid price if not provided but we have bid and ask
            if self.market_data[symbol_value]['mid'] is None:
                bid = self.market_data[symbol_value]['bid']
                ask = self.market_data[symbol_value]['ask']
                self.market_data[symbol_value]['mid'] = (bid + ask) / 2
                
            # Update timestamp
            self.market_data[symbol_value]['timestamp'] = time.time()
            
            # Log successful update with source info
            bid_source = f" [Source: {self.market_data[symbol_value]['bid_source']}]" if self.market_data[symbol_value]['bid_source'] else ""
            ask_source = f" [Source: {self.market_data[symbol_value]['ask_source']}]" if self.market_data[symbol_value]['ask_source'] else ""
            
            logger.info(f"Updated {symbol_value} - bid: {self.market_data[symbol_value]['bid']}{bid_source}, ask: {self.market_data[symbol_value]['ask']}{ask_source}")
            
            # Save individual price to its own file
            self.save_price_to_file(symbol_value, self.market_data[symbol_value])
            
            # Periodically print the price table and save all prices
            if len(self.valid_symbols) > 0 and time.time() % 5 < 0.1:
                self.print_price_table()
                self.save_all_prices_to_file()
            
            # If this is a new symbol with source information, show that we found it
            if source_found and symbol_value not in self.subscribed_symbols:
                logger.info(f"Discovered new symbol with source info: {symbol_value}")
            
        except Exception as e:
            logger.error(f"Error processing market data: {e}")

    def print_price_table(self):
        """Print a formatted table of current prices"""
        # Only print if we have valid symbols
        if not self.valid_symbols:
            return
            
        logger.info("=" * 80)
        logger.info("Current Market Data:")
        logger.info(f"{'Symbol':<12} {'Bid':<12} {'Ask':<12} {'Mid':<12} {'Source':<30}")
        logger.info("-" * 80)
        
        # Only include valid symbols in the sorted list
        sorted_symbols = sorted(self.valid_symbols)
        
        for symbol in sorted_symbols:
            if symbol in self.market_data:
                data = self.market_data[symbol]
                bid = f"{data['bid']:.6f}" if data['bid'] is not None else "N/A"
                ask = f"{data['ask']:.6f}" if data['ask'] is not None else "N/A"
                mid = f"{data['mid']:.6f}" if data['mid'] is not None else "N/A"
                
                # Combine sources if we have both
                source = "Unknown"
                if data['bid_source'] and data['ask_source'] and data['bid_source'] == data['ask_source']:
                    source = data['bid_source']
                elif data['bid_source'] and data['ask_source']:
                    source = f"Bid: {data['bid_source'][:10]}... Ask: {data['ask_source'][:10]}..."
                elif data['bid_source']:
                    source = f"Bid: {data['bid_source']}"
                elif data['ask_source']:
                    source = f"Ask: {data['ask_source']}"
                
                logger.info(f"{symbol:<12} {bid:<12} {ask:<12} {mid:<12} {source:<30}")
        logger.info("=" * 80)
        logger.info(f"Total valid symbols: {len(self.valid_symbols)}")

    def save_price_to_file(self, symbol, data):
        """Save a single price to its own file for quick access"""
        # Only save if it's a valid symbol with complete data
        if symbol not in self.valid_symbols:
            return
            
        try:
            # Create prices directory if it doesn't exist
            if not os.path.exists("prices"):
                os.makedirs("prices")
                
            # Use a safe filename (replace / with _)
            safe_symbol = symbol.replace("/", "_")
            
            # Write to JSON file
            with open(f"prices/{safe_symbol}.json", "w") as f:
                json.dump({
                    "symbol": symbol,
                    "bid": data["bid"],
                    "ask": data["ask"],
                    "mid": data["mid"],
                    "bid_source": data["bid_source"],
                    "ask_source": data["ask_source"],
                    "bid_size": data["bid_size"],
                    "ask_size": data["ask_size"],
                    "timestamp": data["timestamp"]
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving price for {symbol} to file: {e}")

    def save_all_prices_to_file(self):
        """Save all prices to a CSV and JSON file for consumption by other applications"""
        # Only save if we have valid symbols
        if not self.valid_symbols:
            return
            
        try:
            # Only include valid symbols with complete data
            valid_data = {symbol: data for symbol, data in self.market_data.items() 
                         if symbol in self.valid_symbols}
                         
            # Save to CSV
            with open("current_prices.csv", "w") as f:
                f.write("Symbol,Bid,Ask,Mid,BidSource,AskSource,BidSize,AskSize,Timestamp\n")
                for symbol, data in sorted(valid_data.items()):
                    bid = data["bid"] if data["bid"] is not None else ""
                    ask = data["ask"] if data["ask"] is not None else ""
                    mid = data["mid"] if data["mid"] is not None else ""
                    bid_source = f'"{data["bid_source"]}"' if data["bid_source"] else ""
                    ask_source = f'"{data["ask_source"]}"' if data["ask_source"] else ""
                    bid_size = data["bid_size"] if data["bid_size"] is not None else ""
                    ask_size = data["ask_size"] if data["ask_size"] is not None else ""
                    timestamp = data["timestamp"] if data["timestamp"] is not None else ""
                    f.write(f"{symbol},{bid},{ask},{mid},{bid_source},{ask_source},{bid_size},{ask_size},{timestamp}\n")
            
            # Save to JSON
            with open("current_prices.json", "w") as f:
                json.dump({
                    "timestamp": time.time(),
                    "prices": valid_data
                }, f, indent=2)
                
            logger.debug(f"Saved all {len(valid_data)} valid prices to files")
        except Exception as e:
            logger.error(f"Error saving all prices to files: {e}")

    def subscribe_market_data(self, symbol):
        """Send a market data request for a specific symbol"""
        if not self.logged_on:
            logger.error(f"Not logged on, cannot subscribe to market data for {symbol}")
            return False
            
        try:
            logger.info(f"Subscribing to market data for {symbol}")
            
            message = fix.Message()
            header = message.getHeader()
            header.setField(fix.MsgType(fix.MsgType_MarketDataRequest))
            
            # Set MDReqID - must be unique
            message.setField(fix.MDReqID(f"MDR-{symbol}-{int(time.time())}"))
            
            # Subscribe to snapshot
            message.setField(fix.SubscriptionRequestType("1"))  # 1 = Subscribe
            
            # Market depth
            message.setField(fix.MarketDepth(0))  # 0 = Full book
            
            # Update type
            message.setField(fix.MDUpdateType(0))  # 0 = Full refresh
            
            # Entry types
            no_md_entry_types = fix.NoMDEntryTypes(3)
            message.setField(no_md_entry_types)
            
            # Add bid entry type
            group1 = fix.Group(267, 269)
            group1.setField(fix.MDEntryType("0"))  # 0 = Bid
            message.addGroup(group1)
            
            # Add offer entry type
            group2 = fix.Group(267, 269)
            group2.setField(fix.MDEntryType("1"))  # 1 = Offer
            message.addGroup(group2)
            
            # Add mid-point entry type
            group3 = fix.Group(267, 269)
            group3.setField(fix.MDEntryType("2"))  # 2 = Mid (using standard value)
            message.addGroup(group3)
            
            # Add symbol
            no_related_sym = fix.NoRelatedSym(1)
            message.setField(no_related_sym)
            
            sym_group = fix.Group(146, 55)
            sym_group.setField(fix.Symbol(symbol))
            message.addGroup(sym_group)
            
            # Send request
            result = fix.Session.sendToTarget(message, self.session_id)
            if result:
                logger.info(f"Market data request for {symbol} sent successfully")
            else:
                logger.error(f"Failed to send market data request for {symbol}")
            
            return result
        except Exception as e:
            logger.error(f"Error subscribing to market data for {symbol}: {e}")
            return False

    def schedule_discovery(self):
        """Schedule a background thread to periodically discover new symbols"""
        import threading
        
        def discovery_worker():
            while self.running and self.logged_on:
                try:
                    # Periodically try to discover more symbols based on common patterns
                    self.try_common_currency_patterns()
                    
                    # Wait a bit before the next discovery attempt
                    time.sleep(30)
                except Exception as e:
                    logger.error(f"Error in discovery worker: {e}")
                    time.sleep(10)
        
        discovery_thread = threading.Thread(target=discovery_worker)
        discovery_thread.daemon = True
        discovery_thread.start()
        logger.info("Started symbol discovery thread")

    def try_common_currency_patterns(self):
        """Try subscribing to common currency patterns"""
        # Major fiat currencies
        major_currencies = ["USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD"]
        
        # Major crypto currencies
        crypto_currencies = ["BTC", "ETH", "USDT", "USDC", "XRP", "SOL", "ADA", "DOGE"]
        
        # Try all combinations of major pairs
        pairs_to_try = []
        
        # Fiat pairs
        for base in major_currencies:
            for quote in major_currencies:
                if base != quote:
                    pairs_to_try.append(f"{base}/{quote}")
        
        # Crypto/Fiat pairs
        for crypto in crypto_currencies:
            pairs_to_try.append(f"{crypto}/USD")
            pairs_to_try.append(f"{crypto}/USDT")
            pairs_to_try.append(f"{crypto}/USDC")
        
        # Try some non-standard pairs that might be available
        pairs_to_try.extend([
            "BTC/EUR", "ETH/EUR", "BTC/GBP", "ETH/GBP",
            "XAU/USD", "XAG/USD",  # Gold and Silver
            "EUR/BTC", "USD/BTC",   # Inverted crypto pairs
            "ETH/BTC", "XRP/BTC"    # Crypto/Crypto pairs
        ])
        
        # Subscribe to new pairs
        for symbol in pairs_to_try:
            if symbol not in self.subscribed_symbols:
                self.subscribe_market_data(symbol)
                self.subscribed_symbols.add(symbol)
                time.sleep(0.2)  # Don't flood the server

    def stop(self):
        """Stop the application"""
        self.running = False

def signal_handler(sig, frame):
    """Handle Ctrl+C to gracefully shutdown the application"""
    logger.info("Received interrupt signal, shutting down...")
    global running
    running = False

def main():
    global running
    running = True
    
    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Bosonic FIX API Price Feed Client")
    parser.add_argument("--symbols", nargs="+", help="Specific symbols to subscribe to (e.g. EUR/USD USD/JPY)")
    parser.add_argument("--config", default="bosonic_client.cfg", help="Path to FIX configuration file")
    parser.add_argument("--no-discover", action="store_true", help="Disable automatic discovery of all available pairs")
    args = parser.parse_args()
    
    subscribe_all = not args.no_discover
    symbols = args.symbols
    
    logger.info(f"Starting Bosonic Price Feed Client")
    if symbols:
        logger.info(f"Specified symbols: {symbols}")
    if subscribe_all:
        logger.info("Auto-discovery of available pairs is enabled")
    
    try:
        # Create application instance
        settings = fix.SessionSettings(args.config)
        application = PriceFeedApplication(subscribe_all=subscribe_all, symbols=symbols)
        store_factory = fix.FileStoreFactory(settings)
        log_factory = fix.FileLogFactory(settings)
        
        # Create and start the FIX initiator
        initiator = fix.SocketInitiator(application, store_factory, settings, log_factory)
        initiator.start()
        
        logger.info("FIX client started, waiting for logon...")
        
        # Wait for logon (with timeout)
        wait_count = 0
        while not application.logged_on and wait_count < 10 and running:
            time.sleep(1)
            wait_count += 1
        
        if not application.logged_on and running:
            logger.error("Failed to log on within timeout")
            initiator.stop()
            return 1
            
        logger.info("Logged on successfully and started market data subscriptions")
        
        # Main loop - keep running until interrupted
        while running:
            time.sleep(1)
        
        # Cleanup
        logger.info("Shutting down...")
        initiator.stop()
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(main())
