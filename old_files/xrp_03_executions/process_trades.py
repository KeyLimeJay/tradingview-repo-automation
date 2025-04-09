#!/usr/bin/env python
# coding: utf-8

import os
import json
import glob
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from pathlib import Path

class TradeExecutionProcessor:
    """
    Trade Execution Processor
    
    Processes trade execution data files, filters based on configurable criteria,
    and generates reports with the specified columns.
    """
    
    def __init__(self, config_path="config.json"):
        """Initialize the processor with the specified config file"""
        # Load configuration
        self.config = self._load_config(config_path)
        
        # Setup logging
        self._setup_logging()
        
        # Create output directory if it doesn't exist
        os.makedirs(self.config["output"]["path"], exist_ok=True)
        
        self.logger.info(f"Trade Execution Processor initialized with {config_path}")
    
    def _load_config(self, config_path):
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            return config
        except Exception as e:
            print(f"Error loading configuration: {e}")
            raise
    
    def _setup_logging(self):
        """Set up logging based on configuration"""
        log_dir = os.path.dirname(self.config["logging"]["file"])
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, self.config["logging"]["level"]),
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.config["logging"]["file"]),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
    
    def get_input_files(self):
        """Get list of input files matching the pattern in config"""
        file_pattern = os.path.join(
            self.config["data_source"]["path"],
            self.config["data_source"]["file_pattern"]
        )
        files = glob.glob(file_pattern)
        
        if not files:
            self.logger.warning(f"No files found matching {file_pattern}")
        else:
            self.logger.info(f"Found {len(files)} input files")
            for file in files:
                self.logger.debug(f"Found file: {os.path.basename(file)}")
            
        return sorted(files)
    
    def process_files(self):
        """Process all input files and generate filtered output"""
        input_files = self.get_input_files()
        if not input_files:
            self.logger.error("No input files to process")
            return False
        
        # List to hold all dataframes
        all_data = []
        
        # Process each file
        for file_path in input_files:
            self.logger.info(f"Processing file: {file_path}")
            df = self._process_file(file_path)
            if df is not None and len(df) > 0:
                all_data.append(df)
        
        if not all_data:
            self.logger.error("No data found after processing files")
            return False
        
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        self.logger.info(f"Combined data has {len(combined_df)} rows before filtering")
        
        # Apply filters
        filtered_df = self._apply_filters(combined_df)
        self.logger.info(f"After filtering: {len(filtered_df)} rows remain")
        
        # Handle duplicates
        final_df = self._handle_duplicates(filtered_df)
        self.logger.info(f"After removing duplicates: {len(final_df)} rows remain")
        
        # Generate output file name
        date_str = datetime.now().strftime(self.config["output"]["date_format"])
        output_file = self.config["output"]["file_name_pattern"].format(date=date_str)
        
        # Save output
        output_path = os.path.join(
            self.config["output"]["path"],
            output_file
        )
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to CSV
        final_df.to_csv(output_path, index=False)
        self.logger.info(f"Saved filtered data to {output_path}")
        
        # Print summary
        print(f"Processed {len(input_files)} files")
        print(f"Generated report with {len(final_df)} rows after filtering and deduplication")
        print(f"Output saved to: {output_path}")
        
        return True
    
    def _process_file(self, file_path):
        """Process a single input file"""
        try:
            # Read the file
            df = pd.read_csv(file_path)
            
            # Validate required columns
            if "ID" not in df.columns:
                self.logger.error(f"Required column 'ID' missing in file {file_path}")
                return None
            
            # Check if datetime column exists
            if "DateTime" not in df.columns:
                self.logger.error(f"Required column 'DateTime' missing in file {file_path}")
                return None
            
            # Check if we have both Symbol and Side
            if "Symbol" not in df.columns:
                self.logger.error(f"Required column 'Symbol' missing in file {file_path}")
                return None
            
            if "Side" not in df.columns:
                self.logger.error(f"Required column 'Side' missing in file {file_path}")
                return None
            
            # Ensure DateTime is in proper format
            if not pd.api.types.is_datetime64_any_dtype(df["DateTime"]):
                try:
                    df["DateTime"] = pd.to_datetime(df["DateTime"])
                except Exception as e:
                    self.logger.warning(f"Failed to convert DateTime column: {e}")
            
            # Add missing bid/ask columns if needed
            for symbol in self.config["filters"]["symbols"]:
                bid_col = f"{symbol}_BestBid"
                ask_col = f"{symbol}_BestAsk"
                
                # Add missing columns if they're not in the file but are in the config
                if bid_col in self.config["columns"]["include"] and bid_col not in df.columns:
                    # Try to find alternative column naming pattern
                    alt_bid = f"{symbol.replace('USD', '')}_USD_BestBid"
                    if alt_bid in df.columns:
                        df[bid_col] = df[alt_bid]
                    else:
                        self.logger.warning(f"Missing {bid_col} column, adding as empty")
                        df[bid_col] = np.nan
                
                if ask_col in self.config["columns"]["include"] and ask_col not in df.columns:
                    # Try to find alternative column naming pattern
                    alt_ask = f"{symbol.replace('USD', '')}_USD_BestAsk"
                    if alt_ask in df.columns:
                        df[ask_col] = df[alt_ask]
                    else:
                        self.logger.warning(f"Missing {ask_col} column, adding as empty")
                        df[ask_col] = np.nan
            
            self.logger.info(f"Processed {len(df)} rows from {os.path.basename(file_path)}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return None
    
    def _apply_filters(self, df):
        """Apply configured filters to the dataframe"""
        # Make a copy to avoid modifying the original
        filtered_df = df.copy()
        original_count = len(filtered_df)
        
        # Filter by Symbol
        symbols = self.config["filters"]["symbols"]
        if symbols and len(symbols) > 0:
            filtered_df = filtered_df[filtered_df["Symbol"].isin(symbols)]
            self.logger.info(f"Symbol filter: {original_count} -> {len(filtered_df)} rows")
        
        # Filter by Side
        sides = self.config["filters"]["sides"]
        if sides and len(sides) > 0:
            filtered_df = filtered_df[filtered_df["Side"].isin(sides)]
            self.logger.info(f"Side filter: {len(filtered_df)} rows after filtering")
        
        # Filter columns to include only those specified
        include_columns = self.config["columns"]["include"]
        rename_mapping = self.config["columns"]["rename"]
        
        # Check which columns are available
        available_columns = [col for col in include_columns if col in filtered_df.columns]
        missing_columns = [col for col in include_columns if col not in filtered_df.columns]
        
        if missing_columns:
            self.logger.warning(f"Missing columns in data: {', '.join(missing_columns)}")
            # Add missing columns with NaN values
            for col in missing_columns:
                filtered_df[col] = np.nan
        
        # Rename columns if specified
        if rename_mapping:
            filtered_df = filtered_df.rename(columns=rename_mapping)
        
        # Keep only the specified columns
        filtered_df = filtered_df[include_columns]
        
        self.logger.info(f"Final filtered dataframe: {len(filtered_df)} rows with columns: {', '.join(filtered_df.columns)}")
        return filtered_df
    
    def _handle_duplicates(self, df):
        """Handle duplicate IDs by keeping only the last occurrence"""
        if "ID" not in df.columns:
            self.logger.warning("No ID column found for duplicate handling")
            return df
        
        # Check for duplicates
        duplicate_count = df["ID"].duplicated().sum()
        
        if duplicate_count > 0:
            self.logger.info(f"Found {duplicate_count} duplicate IDs")
            
            # Keep last occurrence of each duplicate ID
            cleaned_df = df.drop_duplicates(subset=["ID"], keep="last")
            removed_count = len(df) - len(cleaned_df)
            self.logger.info(f"Removed {removed_count} duplicate rows")
            return cleaned_df
        else:
            self.logger.info("No duplicate IDs found")
            return df

def main():
    """Main function to run the Trade Execution Processor"""
    print("Starting Trade Execution Processor...")
    
    try:
        # Initialize processor
        processor = TradeExecutionProcessor()
        
        # Process files
        success = processor.process_files()
        
        if success:
            print("Trade Execution Processor completed successfully")
        else:
            print("Trade Execution Processor completed with errors, see log for details")
        
    except Exception as e:
        print(f"Error running Trade Execution Processor: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())