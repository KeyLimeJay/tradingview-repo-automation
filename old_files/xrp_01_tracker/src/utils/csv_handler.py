#csv_handler.py
import os
import csv
import datetime
import sys

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from config.settings import DATA_DIR

def get_current_filename():
    """
    Generate a filename based on the current date.
    Format: xrp_trades_YYYY_MM_DD.csv
    """
    today = datetime.datetime.now().strftime('%Y_%m_%d')
    return os.path.join(DATA_DIR, f"xrp_trades_{today}.csv")

def setup_csv(headers):
    """
    Set up the CSV file with custom headers.
    Returns the full path to the CSV file.
    """
    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Get the current date-based filename
    filename = get_current_filename()
    
    # Check if the file exists
    file_exists = os.path.isfile(filename)
    
    with open(filename, 'a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        if not file_exists:
            # New file - add the headers
            writer.writerow(headers)
            print(f"Created new CSV file: {filename}")
        else:
            print(f"Appending to existing CSV file: {filename}")
    
    return filename

def write_to_csv(trade_data, headers):
    """
    Write a single trade record to the current day's CSV file.
    """
    # Get the current date-based filename
    filename = get_current_filename()
    
    # Check if the file exists and create with headers if needed
    file_exists = os.path.isfile(filename)
    
    # Prepare the row data in the correct order
    row = []
    for header in headers:
        row.append(trade_data.get(header, "N/A"))
    
    with open(filename, 'a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        if not file_exists:
            # New file - add the headers
            writer.writerow(headers)
            print(f"Created new CSV file: {filename}")
        
        # Write the data row
        writer.writerow(row)