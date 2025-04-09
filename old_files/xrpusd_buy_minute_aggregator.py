#!/usr/bin/env python3
import json
import csv
import os
import sys
import threading
import time
import datetime
import logging
import numpy as np
import pandas as pd
from collections import defaultdict

import websocket
import requests
import hmac
import hashlib
import uuid

# Bitstamp API Credentials
API_KEY = "VmXQysYrOpSDPNFZEnxJZk6x7pZJRHqk"
API_SECRET = "f7Pw1dR5UCrk9RcCTKb5U09tmep7ORuh"

class TradeDeduplicator:
    """
    Handles deduplication of trade IDs to prevent duplicate entries
    """
    def __init__(self, keep_hours=24):
        """
        Initialize the deduplicator
        
        Args:
            keep_hours (int): Number of hours to keep trade IDs
        """
        self.processed_trade_ids = {}
        self.keep_hours = keep_hours
        self.lock = threading.Lock()
    
    def is_duplicate(self, trade_id):
        """
        Check if a trade ID has already been processed
        
        Args:
            trade_id (str): Unique trade identifier
        
        Returns:
            bool: True if duplicate, False otherwise
        """
        with self.lock:
            return trade_id in self.processed_trade_ids
    
    def add_processed_id(self, trade_id):
        """
        Add a processed trade ID
        
        Args:
            trade_id (str): Unique trade identifier
        """
        current_time = time.time()
        with self.lock:
            self.processed_trade_ids[trade_id] = current_time
    
    def prune_old_ids(self):
        """
        Remove trade IDs older than the specified time period
        """
        current_time = time.time()
        cutoff_time = current_time - (self.keep_hours * 3600)
        
        with self.lock:
            # Find and remove old IDs
            ids_to_remove = [
                trade_id for trade_id, timestamp in self.processed_trade_ids.items() 
                if timestamp < cutoff_time
            ]
            
            for trade_id in ids_to_remove:
                del self.processed_trade_ids[trade_id]
            
            if ids_to_remove:
                logging.info(f"Pruned {len(ids_to_remove)} old trade IDs")

class XRPUSDTradeAggregator:
    def __init__(self, 
                 symbol='XRPUSD', 
                 data_dir='data', 
                 log_level=logging.INFO):
        """
        Initialize the XRP/USD Trade Aggregator
        
        Args:
            symbol (str): Trading symbol to track
            data_dir (str): Directory to store trade data
            log_level (int): Logging level
        """
        # Logging setup
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('xrpusd_trade_aggregator.log')
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Configuration
        self.symbol = symbol
        self.data_dir = data_dir
        
        # Ensure data directory exists
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Deduplication
        self.deduplicator = TradeDeduplicator()
        
        # Tracking variables
        self.current_minute_data = defaultdict(list)
        self.last_processed_minute = None
        self.lock = threading.Lock()
        
        # WebSocket connection tracking
        self.ws = None
        self.running = True
        self.reconnect_count = 0
        self.last_message_time = time.time()
        
        # Order book tracking
        self.order_books = {}
        self.usd_rates = {}
        
        # Trade processing columns (comprehensive)
        self.columns = [
            'ID', 'DateTime', 'Symbol', 'Quantity', 'Rate', 'Side',
            f'{symbol}_BestBid', f'{symbol}_BestAsk',
            'XRPEUR_BestBid', 'XRPEUR_BestAsk',
            'USDEUR_BestBid', 'USDEUR_BestAsk'
        ]
        
        # Initialize CSV
        self._initialize_csv()
    
    def _initialize_csv(self):
        """Initialize CSV file with headers if it doesn't exist"""
        filename = self._get_current_filename()
        
        if not os.path.exists(filename):
            try:
                with open(filename, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(self.columns)
                self.logger.info(f"Created new CSV file: {filename}")
            except Exception as e:
                self.logger.error(f"Error creating CSV file: {e}")
    
    def _get_current_filename(self):
        """Generate current date-based filename"""
        today = datetime.datetime.now().strftime('%Y_%m_%d')
        return os.path.join(self.data_dir, f"{self.symbol.lower()}_trades_{today}.csv")
    
    def _append_to_csv(self, trade_data):
        """Append trade data to current CSV file"""
        try:
            filename = self._get_current_filename()
            
            # Prepare row data in specific order
            row = []
            for col in self.columns:
                # Get value from trade_data, use 'N/A' if not found
                row.append(trade_data.get(col, 'N/A'))
            
            with open(filename, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(row)
            
            self.logger.debug(f"Saved trade: {row}")
        
        except Exception as e:
            self.logger.error(f"Error saving trade to CSV: {e}")
    
    def update_usd_rates(self):
        """
        Update USD rates for all assets based on current order book data
        Inspired by the previous implementation's rate normalization
        """
        with self.lock:
            # Direct USD rates
            for pair, orderbook in self.order_books.items():
                if pair.endswith('usd'):
                    asset = pair[:-3].upper()
                    if orderbook.get('best_bid') and orderbook.get('best_ask'):
                        self.usd_rates[f"{asset}_USD_BestBid"] = orderbook['best_bid']
                        self.usd_rates[f"{asset}_USD_BestAsk"] = orderbook['best_ask']
            
            # Ensure USD/USD rate is always 1
            self.usd_rates['USD_USD_BestBid'] = 1.0
            self.usd_rates['USD_USD_BestAsk'] = 1.0
    
    def process_trades(self):
        """
        Process trades for each completed minute
        Combines aggregation from multiple previous implementations
        """
        while self.running:
            current_time = datetime.datetime.now()
            current_minute = current_time.replace(second=0, microsecond=0)
            
            with self.lock:
                # Periodically prune old trade IDs
                if int(time.time()) % 1000 == 0:
                    self.deduplicator.prune_old_ids()
                
                if self.last_processed_minute and current_minute > self.last_processed_minute:
                    minute_key = self.last_processed_minute.strftime("%Y-%m-%d %H:%M")
                    
                    if self.current_minute_data[minute_key]:
                        # Aggregate trades for the minute
                        price_size_products = 0
                        total_buy_size = 0
                        trades = self.current_minute_data[minute_key]
                        
                        for trade in trades:
                            price_size_products += trade['Rate'] * trade['Quantity']
                            total_buy_size += trade['Quantity']
                        
                        if total_buy_size > 0:
                            vwap = price_size_products / total_buy_size
                            
                            # Prepare aggregated minute data
                            minute_summary = {
                                'ID': f"{minute_key}_summary",
                                'DateTime': self.last_processed_minute.strftime('%Y-%m-%d %H:%M:%S'),
                                'Symbol': self.symbol,
                                'Quantity': total_buy_size,
                                'Rate': vwap,
                                'Side': 'Buy'
                            }
                            
                            # Add order book and USD rates
                            for pair, orderbook in self.order_books.items():
                                if pair.endswith('usd'):
                                    minute_summary[f"{pair.upper()}_BestBid"] = orderbook.get('best_bid')
                                    minute_summary[f"{pair.upper()}_BestAsk"] = orderbook.get('best_ask')
                            
                            # Add USD rates
                            for key, rate in self.usd_rates.items():
                                minute_summary[key] = rate
                            
                            # Log and save aggregated data
                            self.logger.info(
                                f"Minute Summary: {minute_key}, "
                                f"Total Buy Quantity: {total_buy_size}, "
                                f"VWAP: {vwap}, "
                                f"Trade Count: {len(trades)}"
                            )
                            
                            # Append to CSV
                            self._append_to_csv(minute_summary)
                        
                        # Remove processed data
                        del self.current_minute_data[minute_key]
                
                # Update last processed minute
                self.last_processed_minute = current_minute
            
            # Wait before next processing cycle
            time.sleep(5)
    
    def on_message(self, ws, message):
        """
        Handle incoming WebSocket messages
        Combines trade and order book processing
        """
        try:
            # Update last message time
            self.last_message_time = time.time()
            
            data = json.loads(message)
            
            # Trade event processing
            if (data.get('event') == 'trade' and 
                f'live_trades_{self.symbol.lower()}' in data.get('channel', '')):
                
                trade_data = data['data']
                trade_id = str(trade_data['id'])
                
                # Check for duplicates
                if self.deduplicator.is_duplicate(trade_id):
                    self.logger.debug(f"Skipping duplicate trade: {trade_id}")
                    return
                
                # Mark as processed
                self.deduplicator.add_processed_id(trade_id)
                
                # Only process Buy trades (type 0)
                if int(trade_data['type']) == 0:
                    current_time = datetime.datetime.now()
                    minute_key = current_time.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")
                    
                    # Prepare trade dictionary
                    trade_entry = {
                        'ID': trade_id,
                        'DateTime': datetime.datetime.fromtimestamp(int(trade_data['timestamp'])).strftime('%Y-%m-%d %H:%M:%S'),
                        'Symbol': self.symbol,
                        'Quantity': float(trade_data['amount']),
                        'Rate': float(trade_data['price']),
                        'Side': 'Buy'
                    }
                    
                    with self.lock:
                        self.current_minute_data[minute_key].append(trade_entry)
                        
                        # Append to CSV
                        self._append_to_csv(trade_entry)
                    
                    self.logger.debug(f"Buy Trade: {trade_entry}")
            
            # Order book event processing
            elif (data.get('event') == 'data' and 
                  'order_book_' in data.get('channel', '')):
                
                pair = data['channel'].split('_')[2]
                order_book_data = data.get('data', {})
                
                if order_book_data.get('bids') and order_book_data.get('asks'):
                    with self.lock:
                        # Store best bid and ask
                        self.order_books[pair] = {
                            'best_bid': float(order_book_data['bids'][0][0]),
                            'best_ask': float(order_book_data['asks'][0][0])
                        }
                    
                    # Update USD rates
                    self.update_usd_rates()
        
        except Exception as e:
            self.logger.error(f"Message processing error: {e}")
    
    def connect(self):
        """Establish WebSocket connection"""
        while self.running:
            try:
                # WebSocket connection
                self.ws = websocket.WebSocketApp(
                    "wss://ws.bitstamp.net",
                    header=self._get_auth_headers(),
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                
                def on_open(ws):
                    """Subscribe to trades and order books on connection open"""
                    subscriptions = [
                        {
                            "event": "bts:subscribe",
                            "data": {
                                "channel": f"live_trades_{self.symbol.lower()}"
                            }
                        },
                        {
                            "event": "bts:subscribe",
                            "data": {
                                "channel": f"order_book_{self.symbol.lower()}"
                            }
                        }
                    ]
                    
                    for sub in subscriptions:
                        ws.send(json.dumps(sub))
                        self.logger.info(f"Subscribed to {sub['data']['channel']}")
                
                self.ws.on_open = on_open
                
                # Start trade processing thread
                trade_processor = threading.Thread(target=self.process_trades, daemon=True)
                trade_processor.start()
                
                # Run WebSocket
                self.ws.run_forever()
            
            except Exception as e:
                self.logger.error(f"Connection error: {e}")
                time.sleep(5)  # Wait before reconnecting
    
    def _get_auth_headers(self):
        """Generate Bitstamp WebSocket authentication headers"""
        timestamp = str(int(round(time.time() * 1000)))
        nonce = str(uuid.uuid4())
        
        message = f"BITSTAMP {API_KEY}wsswww.bitstamp.netNone{nonce}{timestamp}v2"
        signature = hmac.new(
            API_SECRET.encode('utf-8'), 
            msg=message.encode('utf-8'), 
            digestmod=hashlib.sha256
        ).hexdigest()
        
        return [
            f"X-Auth: BITSTAMP {API_KEY}",
            f"X-Auth-Signature: {signature}",
            f"X-Auth-Nonce: {nonce}",
            f"X-Auth-Timestamp: {timestamp}",
            "X-Auth-Version: v2"
        ]
    
    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        self.logger.error(f"WebSocket Error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection closure"""
        self.logger.warning(f"WebSocket closed: {close_status_code} - {close_msg}")
        
        # Attempt to reconnect
        if self.running:
            self.logger.info("Attempting to reconnect...")
            time.sleep(5)
            self.connect()
    
    def stop(self):
        """Gracefully stop the aggregator"""
        self.running = False
        if self.ws:
            self.ws.close()
        self.logger.info("Aggregator stopped")

def main():
    """Main function to run the XRP/USD Trade Aggregator"""
    try:
        # Initialize aggregator
        aggregator = XRPUSDTradeAggregator()
        
        # Register signal handlers for graceful shutdown
        import signal
        def signal_handler(signum, frame):
            """Handle interrupt signals"""
            print("\nShutting down XRP/USD Trade Aggregator...")
            aggregator.stop()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Start the aggregator
        aggregator.connect()
    
    except KeyboardInterrupt:
        print("\nStopping XRP/USD Trade Aggregator...")
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()