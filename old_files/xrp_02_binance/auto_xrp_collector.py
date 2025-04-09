#!/usr/bin/env python
# coding: utf-8

import os
import requests
import pandas as pd
import numpy as np
import json
import time
from datetime import datetime, timedelta
import zipfile
import io
import logging
import sys
from pathlib import Path

# Setup logging
log_dir = Path('./logs')
log_dir.mkdir(exist_ok=True)
log_file = log_dir / f"xrp_collector_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Binance data configuration
BINANCE_CONFIG = {
    # Base URL for Binance data repository
    "base_url": "https://data.binance.vision/data",
    
    # XRP data configuration
    "data_config": {
        "symbol": "XRPUSDT",
        "interval": "1m",  # Options: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1mo
        "market_type": "spot"  # Options: spot, futures/um, futures/cm
    },
    
    # Data retrieval configuration
    "retrieval": {
        "days_to_check": 7,  # Number of days to check backward for missing data
        "sleep_time": 1.0,   # Sleep time between requests to avoid rate limiting
        "max_retries": 3     # Maximum number of retry attempts for failed downloads
    },
    
    # Local storage configuration
    "storage": {
        "save_path": "./data/xrp_binance/",
        "file_format": "csv",
        "combined_file": "xrp_binance_minute_all.csv",  # Main file that will contain all data
        "save_daily_files": True                        # Whether to save individual daily files
    }
}

# Column names for the klines data
COLUMNS = [
    "open_time", "open", "high", "low", "close", 
    "volume", "close_time", "quote_volume", 
    "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"
]

# Columns to include in final output (remove close_time)
OUTPUT_COLUMNS = [
    "open_time", "open", "high", "low", "close", 
    "volume", "quote_volume", 
    "count", "taker_buy_volume", "taker_buy_quote_volume", "ignore"
]

def create_directory(directory_path):
    """Create directory if it doesn't exist"""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logger.info(f"Created directory: {directory_path}")
    return directory_path

def download_binance_data(symbol, interval, date, market_type="spot", max_retries=3):
    """
    Download klines data from Binance data repository for a specific date
    with retry logic for network issues
    """
    # Format date for URL construction
    if isinstance(date, str):
        date = datetime.strptime(date, "%Y-%m-%d")
    
    date_str = date.strftime("%Y-%m-%d")
    month_str = date.strftime("%Y-%m")
    
    # Construct data repository URL
    base_url = BINANCE_CONFIG["base_url"]
    data_type = "klines"
    
    # First try daily data file
    daily_url = f"{base_url}/{market_type}/daily/{data_type}/{symbol.upper()}/{interval}/{symbol.upper()}-{interval}-{date_str}.zip"
    logger.info(f"Attempting to download daily data from: {daily_url}")
    
    # Retry logic for daily data
    daily_df = None
    retry_count = 0
    
    while retry_count < max_retries and daily_df is None:
        try:
            response = requests.get(daily_url, timeout=30)
            response.raise_for_status()
            
            # Process ZIP file
            z = zipfile.ZipFile(io.BytesIO(response.content))
            csv_name = z.namelist()[0]
            
            # Read CSV data
            with z.open(csv_name) as f:
                daily_df = pd.read_csv(f, header=None, names=COLUMNS)
                logger.info(f"Downloaded daily data successfully for {date_str}")
                return daily_df
                
        except requests.exceptions.HTTPError as e:
            # Only 404 errors should proceed to monthly data
            if response.status_code == 404:
                logger.info(f"Daily data file not found (404), trying monthly data...")
                break
            else:
                # Retry other HTTP errors
                retry_count += 1
                if retry_count < max_retries:
                    logger.warning(f"HTTP error: {e}. Retrying daily data download ({retry_count}/{max_retries})...")
                    time.sleep(2)  # Add delay before retry
                else:
                    logger.error(f"Maximum retries reached for daily data. HTTP error: {e}")
                    
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            # Network errors should be retried
            retry_count += 1
            if retry_count < max_retries:
                logger.warning(f"Network error: {e}. Retrying daily data download ({retry_count}/{max_retries})...")
                time.sleep(5)  # Longer delay for network issues
            else:
                logger.error(f"Maximum retries reached for daily data. Network error: {e}")
                
        except Exception as e:
            # Other errors should be logged and retried
            retry_count += 1
            if retry_count < max_retries:
                logger.warning(f"Unexpected error: {e}. Retrying daily data download ({retry_count}/{max_retries})...")
                time.sleep(2)
            else:
                logger.error(f"Maximum retries reached for daily data. Error: {e}")
    
    # If daily data not found or failed after retries, try monthly data
    if daily_df is None:
        monthly_url = f"{base_url}/{market_type}/monthly/{data_type}/{symbol.upper()}/{interval}/{symbol.upper()}-{interval}-{month_str}.zip"
        logger.info(f"Attempting to download monthly data from: {monthly_url}")
        
        # Retry logic for monthly data
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                response = requests.get(monthly_url, timeout=60)  # Monthly files might be larger
                response.raise_for_status()
                
                # Process ZIP file
                z = zipfile.ZipFile(io.BytesIO(response.content))
                csv_name = z.namelist()[0]
                
                # Read CSV data
                with z.open(csv_name) as f:
                    monthly_df = pd.read_csv(f, header=None, names=COLUMNS)
                    
                    # Filter by date - use numeric comparison for timestamps
                    # Extract first 10 digits of timestamp for comparison
                    monthly_df['open_time_adj'] = monthly_df['open_time'].astype(str).str[:10].astype(float)
                    
                    # Get day range for filtering
                    day_start = int(datetime.combine(date.date(), datetime.min.time()).timestamp())
                    day_end = int(datetime.combine(date.date() + timedelta(days=1), datetime.min.time()).timestamp())
                    
                    # Filter by day
                    filtered_df = monthly_df[(monthly_df['open_time_adj'] >= day_start) & 
                                             (monthly_df['open_time_adj'] < day_end)].copy()
                    
                    # Remove temporary column
                    if 'open_time_adj' in filtered_df:
                        filtered_df = filtered_df.drop(columns=['open_time_adj'])
                    
                    if len(filtered_df) > 0:
                        logger.info(f"Extracted {len(filtered_df)} records for {date_str} from monthly data")
                        return filtered_df
                    else:
                        logger.warning(f"No data found for {date_str} in monthly file")
                        return None
                    
            except (requests.exceptions.RequestException, zipfile.BadZipFile) as e:
                retry_count += 1
                if retry_count < max_retries:
                    logger.warning(f"Error retrieving monthly data: {e}. Retrying ({retry_count}/{max_retries})...")
                    time.sleep(3)
                else:
                    logger.error(f"Maximum retries reached for monthly data. Error: {e}")
                    return None
                    
            except Exception as e:
                logger.error(f"Unexpected error processing monthly data: {e}")
                return None
    
    # If we reach here, both daily and monthly approaches failed
    logger.error(f"Failed to retrieve data for {date_str} after all attempts")
    return None

def preprocess_dataframe(df):
    """
    Preprocess the Binance data dataframe
    - Truncate open_time to 10 digits
    - Remove close_time
    - Convert numeric columns
    """
    if df is None or len(df) == 0:
        return None
    
    # Make a copy to avoid modifying the original
    df = df.copy()
    
    # Standardize open_time to exactly 10 digits
    if 'open_time' in df.columns:
        # Convert to string first
        df['open_time'] = df['open_time'].astype(str)
        # Take only the first 10 digits
        df['open_time'] = df['open_time'].str[:10]
        # Convert back to numeric
        df['open_time'] = pd.to_numeric(df['open_time'], errors='coerce')
    
    # Convert numeric columns
    numeric_cols = ["open", "high", "low", "close", "volume", 
                   "quote_volume", "count", "taker_buy_volume", 
                   "taker_buy_quote_volume"]
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Drop close_time column
    if 'close_time' in df.columns:
        df = df.drop(columns=['close_time'])
    
    return df

def save_daily_dataframe(df, date, path, file_format="csv"):
    """Save daily DataFrame to file"""
    if df is None or len(df) == 0:
        return None
        
    date_str = date.strftime("%Y%m%d")
    daily_file = os.path.join(path, f"xrp_binance_minute_{date_str}.{file_format}")
    
    if file_format.lower() == "csv":
        df.to_csv(daily_file, index=False)
    elif file_format.lower() == "json":
        df.to_json(daily_file, orient="records")
    elif file_format.lower() == "excel" or file_format.lower() == "xlsx":
        df.to_excel(daily_file, index=False)
    else:
        logger.warning(f"Unsupported file format: {file_format}. Saving as CSV instead.")
        daily_file = daily_file.replace(f".{file_format}", ".csv")
        df.to_csv(daily_file, index=False)
    
    logger.info(f"Daily data saved to {daily_file}")
    return daily_file

def unix_to_datetime(unix_timestamp):
    """Convert Unix timestamp to datetime"""
    try:
        # Convert to integer
        unix_timestamp = int(float(unix_timestamp))
        # Convert using utcfromtimestamp
        return datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.error(f"Error converting timestamp {unix_timestamp}: {e}")
        return None

def update_combined_file(df, combined_file_path):
    """
    Update the combined data file with new data - FIXED VERSION
    - If file exists, read it and append new data
    - If file doesn't exist, create it with the new data
    - Remove duplicates based on open_time
    - Sort by open_time
    - Convert open_time to datetime format
    """
    if df is None or len(df) == 0:
        logger.warning("No data to append to combined file")
        return False
        
    try:
        # Create a copy to avoid modifying the original
        df_copy = df.copy()
        
        # Convert timestamps to datetime strings
        df_copy['open_time'] = df_copy['open_time'].apply(unix_to_datetime)
        
        # Initialize the combined dataframe
        combined_df = None
        
        # Check if combined file exists
        if os.path.exists(combined_file_path):
            # Read existing data
            try:
                existing_df = pd.read_csv(combined_file_path)
                logger.info(f"Read existing combined file with {len(existing_df)} rows")
                
                # Combine with new data
                combined_df = pd.concat([existing_df, df_copy], ignore_index=True)
                logger.info(f"Combined dataframe has {len(combined_df)} rows before deduplication")
                
            except Exception as e:
                logger.error(f"Error reading existing file: {e}")
                logger.warning("Creating new combined file instead")
                combined_df = df_copy
        else:
            # File doesn't exist, use new data
            combined_df = df_copy
            logger.info(f"Creating new combined file with {len(df_copy)} rows")
        
        # Remove duplicates based on open_time if needed
        if combined_df is not None and len(combined_df) > 0:
            # Check for duplicates
            if 'open_time' in combined_df.columns:
                # Count rows before deduplication
                before_count = len(combined_df)
                
                # Drop duplicates
                combined_df = combined_df.drop_duplicates(subset=['open_time'], keep='first')
                
                # Count rows after deduplication
                after_count = len(combined_df)
                dups_removed = before_count - after_count
                
                if dups_removed > 0:
                    logger.info(f"Removed {dups_removed} duplicate rows")
            
            # Sort by open_time
            combined_df = combined_df.sort_values(by='open_time')
            
            # Ensure we have all required columns
            for col in OUTPUT_COLUMNS:
                if col not in combined_df.columns:
                    if col == 'open_time':
                        logger.error("Critical column 'open_time' missing!")
                        return False
                    else:
                        # Add missing columns with NaN values
                        combined_df[col] = np.nan
            
            # Keep only the columns we need
            combined_df = combined_df[OUTPUT_COLUMNS]
            
            # Save to file
            combined_df.to_csv(combined_file_path, index=False)
            logger.info(f"Saved combined file with {len(combined_df)} total rows")
            
            return True
        else:
            logger.error("Failed to create combined DataFrame")
            return False
            
    except Exception as e:
        logger.error(f"Error updating combined file: {e}")
        # Print full traceback for debugging
        import traceback
        logger.error(traceback.format_exc())
        return False

def check_date_exists(date, combined_file_path):
    """Check if data for a specific date already exists in the combined file"""
    if not os.path.exists(combined_file_path):
        return False
        
    try:
        # Read the combined file
        df = pd.read_csv(combined_file_path)
        
        # Convert the date parameter to datetime
        if isinstance(date, str):
            date_obj = datetime.strptime(date, "%Y-%m-%d")
        else:
            date_obj = date
        
        # Check if the DataFrame is empty
        if len(df) == 0:
            return False
            
        # Convert open_time to datetime for comparison
        df['open_time'] = pd.to_datetime(df['open_time'])
        
        # Extract date part only
        df['date'] = df['open_time'].dt.date
        date_check = date_obj.date()
        
        # Check if any rows match the date
        return (df['date'] == date_check).any()
        
    except Exception as e:
        logger.error(f"Error checking if date exists: {e}")
        return False

def get_missing_dates(combined_file_path, days_to_check=5):
    """
    Get a list of missing dates in the specified period
    - Checks from today going back 'days_to_check' days
    - Returns dates that don't have data in the combined file
    """
    missing_dates = []
    today = datetime.now()
    
    for i in range(days_to_check):
        check_date = today - timedelta(days=i)
        if not check_date_exists(check_date, combined_file_path):
            missing_dates.append(check_date.date())
    
    return missing_dates

def main():
    """Main function to run data collection process"""
    logger.info("Starting XRP data collection process...")
    
    # Create storage directory
    storage_path = create_directory(BINANCE_CONFIG["storage"]["save_path"])
    
    # Path to combined file
    combined_file_path = os.path.join(storage_path, BINANCE_CONFIG["storage"]["combined_file"])
    
    # Get missing dates
    days_to_check = BINANCE_CONFIG["retrieval"]["days_to_check"]
    missing_dates = get_missing_dates(combined_file_path, days_to_check)
    
    if not missing_dates:
        logger.info(f"No missing data found in the last {days_to_check} days.")
        return
    
    logger.info(f"Found {len(missing_dates)} days with missing data: {missing_dates}")
    
    # Track success rate
    successful_dates = 0
    failed_dates = 0
    
    # Download and process data for each missing date
    for date in missing_dates:
        logger.info(f"Processing data for {date}...")
        
        # Download data with retry logic
        df = download_binance_data(
            symbol=BINANCE_CONFIG["data_config"]["symbol"],
            interval=BINANCE_CONFIG["data_config"]["interval"],
            date=date,
            market_type=BINANCE_CONFIG["data_config"]["market_type"],
            max_retries=BINANCE_CONFIG["retrieval"]["max_retries"]
        )
        
        # Process data if available
        if df is not None and len(df) > 0:
            # Preprocess dataframe
            df = preprocess_dataframe(df)
            
            if df is not None and len(df) > 0:
                # Save daily file if configured
                if BINANCE_CONFIG["storage"]["save_daily_files"]:
                    save_daily_dataframe(
                        df, 
                        date, 
                        storage_path, 
                        BINANCE_CONFIG["storage"]["file_format"]
                    )
                
                # Update combined file (always do this)
                update_success = update_combined_file(df, combined_file_path)
                
                if update_success:
                    logger.info(f"Successfully processed {len(df)} records for {date}")
                    successful_dates += 1
                else:
                    logger.error(f"Failed to update combined file for {date}")
                    failed_dates += 1
            else:
                logger.warning(f"No valid data after preprocessing for {date}")
                failed_dates += 1
        else:
            logger.warning(f"No data retrieved for {date}")
            failed_dates += 1
        
        # Sleep to avoid hitting rate limits
        time.sleep(BINANCE_CONFIG["retrieval"]["sleep_time"])
    
    # Log summary
    logger.info(f"Data collection summary: {successful_dates} dates successful, {failed_dates} dates failed")
    
    # Verify combined file
    if os.path.exists(combined_file_path):
        try:
            df = pd.read_csv(combined_file_path)
            df['open_time'] = pd.to_datetime(df['open_time'])
            date_counts = df.groupby(df['open_time'].dt.date).size()
            
            logger.info("Combined file date counts:")
            for date, count in date_counts.items():
                logger.info(f"  {date}: {count} records")
            
            date_range = f"{df['open_time'].min()} to {df['open_time'].max()}"
            logger.info(f"Combined file contains data from {date_range}")
            logger.info(f"Total records in combined file: {len(df)}")
        except Exception as e:
            logger.error(f"Error analyzing combined file: {e}")
    
    logger.info("Data collection process completed!")

if __name__ == "__main__":
    main()