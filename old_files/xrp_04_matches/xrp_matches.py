import os
import pandas as pd
import glob

# Define file paths
binance_file = "/opt/otcxn/tradingview-repo-automation/xrp_02_binance/data/xrp_binance/xrp_binance_minute_all.csv"
executions_file = "/opt/otcxn/tradingview-repo-automation/xrp_03_executions/data/reports/combined_trades.csv"
output_dir = "/opt/otcxn/tradingview-repo-automation/xrp_04_matches/output"

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Read Binance data
print(f"Reading Binance data from: {binance_file}")
binance_df = pd.read_csv(binance_file)

# Read execution data
print(f"Reading execution data from: {executions_file}")
execution_df = pd.read_csv(executions_file)

# Print column names to verify
print("\nBinance columns:", binance_df.columns.tolist())
print("Execution columns:", execution_df.columns.tolist())

# Print data types of timestamp columns
print("\nBinance open_time dtype:", binance_df['open_time'].dtype)
print("Execution DateTime dtype:", execution_df['DateTime'].dtype)

# Show a few sample rows from each dataset
print("\nBinance sample data (first 3 rows):")
print(binance_df[['open_time']].head(3))
print("\nExecution sample data (first 3 rows):")
print(execution_df[['DateTime']].head(3))

# Format the datetime columns for joining at minute level (without seconds)
# For Binance data
binance_df['join_time'] = pd.to_datetime(binance_df['open_time']).dt.strftime('%Y-%m-%d %H:%M:00')

# For Execution data (truncate seconds to 00)
execution_df['join_time'] = pd.to_datetime(execution_df['DateTime']).dt.strftime('%Y-%m-%d %H:%M:00')

# Print date ranges
print("\nBinance date range:")
print(f"Min: {binance_df['join_time'].min()}")
print(f"Max: {binance_df['join_time'].max()}")

print("\nExecution date range:")
print(f"Min: {execution_df['join_time'].min()}")
print(f"Max: {execution_df['join_time'].max()}")

# Show number of unique join values in each dataset
binance_unique = binance_df['join_time'].unique()
execution_unique = execution_df['join_time'].unique()

print(f"\nNumber of unique join times in Binance data: {len(binance_unique)}")
print(f"Number of unique join times in Execution data: {len(execution_unique)}")

# Find intersection of join times
common_times = set(binance_unique).intersection(set(execution_unique))
print(f"Number of common join times: {len(common_times)}")

if len(common_times) > 0:
    print("Sample common join times (up to 5):")
    for time in list(common_times)[:5]:
        print(f"  {time}")
else:
    print("No common join times found!")
    
    # Additional diagnostics if no common times
    print("\nFirst 5 Binance join times:")
    for time in sorted(binance_unique)[:5]:
        print(f"  {time}")
    
    print("\nFirst 5 Execution join times:")
    for time in sorted(execution_unique)[:5]:
        print(f"  {time}")

# Perform inner join
print("\nPerforming inner join...")
merged_df = pd.merge(
    binance_df,
    execution_df,
    on='join_time',
    how='inner'
)

print(f"Rows after merge (before dropping nulls): {len(merged_df)}")

# Drop the join column
if len(merged_df) > 0:
    merged_df = merged_df.drop('join_time', axis=1)

# Check for nulls before dropping
if len(merged_df) > 0:
    null_counts = merged_df.isnull().sum()
    print("\nNull value counts by column:")
    for col, count in null_counts.items():
        if count > 0:
            print(f"  {col}: {count} nulls")

# Save the merged data with a short name
short_output_file = os.path.join(output_dir, "xrp_matched.csv")
if len(merged_df) > 0:
    merged_df.to_csv(short_output_file, index=False)
    print(f"\nMerged data saved to: {short_output_file}")

# Drop rows with null values
merged_df_no_nulls = merged_df.dropna()

# Save to output file with a short name for the version without nulls
no_nulls_output_file = os.path.join(output_dir, "xrp_matched_no_nulls.csv")
merged_df_no_nulls.to_csv(no_nulls_output_file, index=False)

print(f"\nFinal results:")
print(f"Original rows - Binance: {len(binance_df)}, Execution: {len(execution_df)}")
print(f"Merged rows before dropping nulls: {len(merged_df)}")
print(f"Merged rows after dropping nulls: {len(merged_df_no_nulls)}")

# Try a more flexible join approach if no matches found
if len(merged_df) == 0:
    print("\n=== Attempting more flexible join approach ===")
    
    # Extract just the date part
    binance_df['join_date'] = pd.to_datetime(binance_df['open_time']).dt.strftime('%Y-%m-%d')
    execution_df['join_date'] = pd.to_datetime(execution_df['DateTime']).dt.strftime('%Y-%m-%d')
    
    # Count unique dates
    binance_dates = binance_df['join_date'].unique()
    execution_dates = execution_df['join_date'].unique()
    common_dates = set(binance_dates).intersection(set(execution_dates))
    
    print(f"Number of unique dates in Binance: {len(binance_dates)}")
    print(f"Number of unique dates in Execution: {len(execution_dates)}")
    print(f"Number of common dates: {len(common_dates)}")
    
    if len(common_dates) > 0:
        print("Common dates found:")
        for date in sorted(common_dates):
            print(f"  {date}")
        
        # Try joining just on the date
        print("\nJoining on date only (will create cartesian product)...")
        date_merged_df = pd.merge(
            binance_df,
            execution_df,
            on='join_date',
            how='inner'
        )
        
        print(f"Date-only join produced {len(date_merged_df)} rows")
        
        if len(date_merged_df) > 0:
            # Save date-only join with a shorter name
            date_join_file = os.path.join(output_dir, "xrp_date_joined.csv")
            date_merged_df.head(100).to_csv(date_join_file, index=False)
            print(f"Sample of date-only join saved to: {date_join_file}")