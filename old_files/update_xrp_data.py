import csv
import time
import os
import datetime

def process_and_join_data():
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"Running data processing job at {current_time}")
    
    # Path to the files - adjust these paths based on your actual file locations
    trades_file = '/opt/otcxn/tradingview-repo-automation/data/xrpusd_trades_2025_04_08.csv'
    aggregated_file = '/opt/otcxn/tradingview-repo-automation/XRP_USDT_minute_aggregated.csv'
    
    try:
        # Check if files exist
        if not os.path.exists(trades_file):
            print(f"ERROR: Trades file not found at {trades_file}")
            print(f"Current directory: {os.getcwd()}")
            print("Listing files in data directory:")
            if os.path.exists('/opt/otcxn/tradingview-repo-automation/data'):
                print(os.listdir('/opt/otcxn/tradingview-repo-automation/data'))
            return
        if not os.path.exists(aggregated_file):
            print(f"ERROR: Aggregated file not found at {aggregated_file}")
            return
            
        print(f"Found trades file: {trades_file}")
        print(f"Found aggregated file: {aggregated_file}")
        
        # Read trades file and filter summary rows
        summary_rows = []
        with open(trades_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if '_summary' in row.get('ID', ''):
                    # Keep only required columns
                    filtered_row = {
                        'DateTime': row['DateTime'],
                        'Symbol': row['Symbol'],
                        'Quantity': row['Quantity'],
                        'Rate': row['Rate'],
                        'Side': row['Side']
                    }
                    summary_rows.append(filtered_row)
        
        # Sort by DateTime
        summary_rows.sort(key=lambda x: x['DateTime'])
        
        # Read aggregated file
        aggregated_rows = []
        with open(aggregated_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Convert timestamp to match format
                dt = row['timestamp'].replace('T', ' ').split('.')[0]
                aggregated_rows.append({
                    'DateTime': dt[:16] + ':00',  # Format to match summary rows
                    'symbol': row['symbol'],
                    'total_bid_size': row['total_bid_size'],
                    'vwap': row['vwap']
                })
        
        # Create a lookup for aggregated data
        aggregated_dict = {row['DateTime']: row for row in aggregated_rows}
        
        # Join the data
        joined_rows = []
        for summary_row in summary_rows:
            joined_row = dict(summary_row)
            agg_row = aggregated_dict.get(summary_row['DateTime'])
            if agg_row:
                joined_row['symbol_agg'] = agg_row['symbol']
                joined_row['total_bid_size'] = agg_row['total_bid_size']
                joined_row['vwap'] = agg_row['vwap']
            else:
                joined_row['symbol_agg'] = ''
                joined_row['total_bid_size'] = ''
                joined_row['vwap'] = ''
            joined_rows.append(joined_row)
        
        # Convert column names to lowercase
        lowercase_rows = []
        for row in joined_rows:
            lowercase_row = {}
            for key, value in row.items():
                lowercase_row[key.lower()] = value
            lowercase_rows.append(lowercase_row)
        
        # Save the result to a new file
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'/opt/otcxn/tradingview-repo-automation/joined_xrp_data_{timestamp}.csv'
        standard_output = '/opt/otcxn/tradingview-repo-automation/joined_xrp_data_latest.csv'
        
        # Write output file
        if lowercase_rows:
            with open(output_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=lowercase_rows[0].keys())
                writer.writeheader()
                writer.writerows(lowercase_rows)
            
            # Write standard output file
            with open(standard_output, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=lowercase_rows[0].keys())
                writer.writeheader()
                writer.writerows(lowercase_rows)
            
            print(f"Processing complete at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Output saved to {output_file} and {standard_output}")
            print(f"Joined data rows: {len(lowercase_rows)}")
        else:
            print("No data to write after processing.")
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        import traceback
        traceback.print_exc()

# Run the function once
process_and_join_data()

# Setup a simple loop to run every 10 minutes
print("Script started. Will process data every 10 minutes...")
while True:
    time.sleep(600)  # Sleep for 10 minutes
    process_and_join_data()