#matcher.py
"""
Matcher module for matching executions with bids.
"""
import os
import csv
import time
import threading
import datetime
import logging
from typing import Tuple, Dict, Any, List
from config.config import OUTPUT_DIR, OUTPUT_FILE

logger = logging.getLogger('xrp_matcher.matcher')

class Matcher:
    """Matcher class for processing executions and bids."""
    
    def __init__(self, execution_queue, bid_queue, running_event):
        """
        Initialize the matcher.
        
        Args:
            execution_queue: Queue for incoming executions
            bid_queue: Queue for incoming bids
            running_event: Event to signal when to stop
        """
        self.execution_queue = execution_queue
        self.bid_queue = bid_queue
        self.running_event = running_event
        self.available_bids = []  # List to store available bids
        self.output_filename = self.setup_output_file()
        self.matcher_thread = None
    
    def setup_output_file(self):
        """Set up the output CSV file with headers."""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # Use a fixed filename without date
        filename = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
        
        # Define headers
        headers = [
            "ID",                    # Execution ID
            "DateTime",              # Execution DateTime
            "Symbol",                # Execution Symbol
            "Side",                  # Execution Side (Buy)
            "Quantity",              # Original Execution Quantity
            "Rate",                  # Execution Rate
            "fill_counter",          # Counter for partial fills
            "fill_type",             # "fill" or "partial_fill"
            "fill_quantity",         # Quantity for this specific fill
            "remaining_quantity",    # Quantity remaining after this fill
            "bid_timestamp",         # Timestamp of the matched bid
            "bid_symbol",            # Symbol of the matched bid
            "bid_price",             # Price of the matched bid
            "bid_size",              # Original size of the matched bid
            "bid_remaining_size"     # Remaining size of the matched bid after this fill
        ]
        
        # Check if file exists
        file_exists = os.path.isfile(filename)
        
        # Create or check file
        with open(filename, 'a', newline='') as csv_file:
            writer = csv.writer(csv_file)
            if not file_exists:
                writer.writerow(headers)
                logger.info(f"Created new output file: {filename}")
            else:
                logger.info(f"Using existing output file: {filename}")
        
        return filename
    
    def save_match_to_csv(self, match):
        """Write a match record to the CSV file."""
        row = [
            match['ID'],
            match['DateTime'],
            match['Symbol'],
            match['Side'],
            match['Quantity'],
            match['Rate'],
            match['fill_counter'],
            match['fill_type'],
            match['fill_quantity'],
            match['remaining_quantity'],
            match['bid_timestamp'],
            match['bid_symbol'],
            match['bid_price'],
            match['bid_size'],
            match['bid_remaining_size']
        ]
        
        with open(self.output_filename, 'a', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(row)
        
        # Log the match
        logger.info(f"Match saved - ID: {match['ID']}, "
                   f"Fill type: {match['fill_type']}, "
                   f"Fill quantity: {match['fill_quantity']}")
    
    def find_closest_bid(self, execution_time) -> Tuple[int, dict]:
        """
        Find the bid with the closest timestamp to the execution time.
        
        Args:
            execution_time (datetime): The timestamp of the execution
            
        Returns:
            Tuple[int, dict]: Index and bid data of the closest available bid, 
                             or (None, None) if no bids available
        """
        # Filter bids with remaining size > 0
        valid_bids = [(i, bid) for i, bid in enumerate(self.available_bids) 
                      if bid['remaining_size'] > 0]
        
        if not valid_bids:
            return None, None
        
        # Calculate time differences and find minimum
        closest_idx = None
        closest_bid = None
        min_diff = float('inf')
        
        for i, bid in valid_bids:
            time_diff = abs((bid['timestamp'] - execution_time).total_seconds())
            if time_diff < min_diff:
                min_diff = time_diff
                closest_idx = i
                closest_bid = bid
        
        return closest_idx, closest_bid
    
    def process_execution(self, execution):
        """
        Process an execution by matching it with available bids.
        
        Args:
            execution (dict): The execution to process
        
        Returns:
            bool: True if matches were found, False otherwise
        """
        execution_id = execution['id']
        execution_time = execution['datetime']
        execution_quantity = execution['amount']
        remaining_quantity = execution_quantity
        
        logger.info(f"Processing execution ID: {execution_id}, Quantity: {execution_quantity}")
        
        match_counter = 1
        matches_found = False
        
        # Continue until the entire execution quantity is filled
        while remaining_quantity > 0:
            # Find the closest available bid
            bid_idx, bid = self.find_closest_bid(execution_time)
            
            if bid is None:
                logger.warning(f"No more bids available to fill execution {execution_id}")
                break
            
            bid_size = bid['remaining_size']
            
            # Determine fill amount and update remaining quantities
            fill_quantity = min(remaining_quantity, bid_size)
            remaining_quantity -= fill_quantity
            self.available_bids[bid_idx]['remaining_size'] = bid_size - fill_quantity
            
            # Determine fill type
            if match_counter == 1 and remaining_quantity == 0:
                fill_type = "fill"
            else:
                fill_type = "partial_fill"
            
            # Create match record
            match = {
                'ID': execution_id,
                'DateTime': execution['datetime_str'],
                'Symbol': execution['symbol'],
                'Side': execution['side'],
                'Quantity': execution_quantity,  # Original quantity
                'Rate': execution['price'],
                'fill_counter': match_counter,
                'fill_type': fill_type,
                'fill_quantity': fill_quantity,
                'remaining_quantity': remaining_quantity,
                'bid_timestamp': bid['timestamp_str'],
                'bid_symbol': bid['symbol'],
                'bid_price': bid['bid_price'],
                'bid_size': bid['bid_size'],  # Original bid size
                'bid_remaining_size': self.available_bids[bid_idx]['remaining_size']
            }
            
            # Save the match
            self.save_match_to_csv(match)
            matches_found = True
            
            logger.info(f"  Match {match_counter}: {fill_type}, Quantity: {fill_quantity}, "
                       f"Bid remaining: {self.available_bids[bid_idx]['remaining_size']}")
            
            match_counter += 1
        
        return matches_found
    
    def run(self):
        """Process executions and bids continuously."""
        logger.info("Matcher thread started")
        
        try:
            while self.running_event.is_set():
                # Process any new bids
                while not self.bid_queue.empty():
                    bid = self.bid_queue.get()
                    self.available_bids.append(bid)
                    # Sort bids by timestamp
                    self.available_bids.sort(key=lambda x: x['timestamp'])
                    
                # Process any new executions
                while not self.execution_queue.empty():
                    execution = self.execution_queue.get()
                    self.process_execution(execution)
                    
                # Clean up old bids
                # This removes fully used bids or very old bids
                current_time = datetime.datetime.now()
                self.available_bids = [bid for bid in self.available_bids 
                                     if (bid['remaining_size'] > 0 and 
                                        (current_time - bid['timestamp']).total_seconds() < 3600)]
                
                # Sleep to avoid busy waiting
                time.sleep(0.1)
                
        except Exception as e:
            logger.error(f"Error in matcher thread: {e}")
            self.running_event.clear()
    
    def start(self):
        """Start the matcher thread."""
        self.matcher_thread = threading.Thread(target=self.run)
        self.matcher_thread.daemon = True
        self.matcher_thread.start()
        return self.matcher_thread
    
    def stop(self):
        """Stop the matcher thread."""
        # The thread will stop when the running event is cleared
        pass
