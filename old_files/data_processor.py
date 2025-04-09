#!/usr/bin/env python
# coding: utf-8

"""
XRP Data Processing Module
Functions for processing XRP trading data
"""

import pandas as pd
from datetime import datetime, timedelta

def process_xrp_data(result_df):
    """
    Process the XRP data with verification at each step to ensure no data is lost unexpectedly.
    """
    # Print shape for verification
    print(f"Starting with {result_df.shape[0]} rows")
    
    # Select only the columns we want to keep, in the specified order
    selected_df = result_df[['open_time', 'ID', 'DateTime', 'Symbol', 'Quantity', 
                            'Rate', 'Side', 'XRPUSD_BestBid', 'XRPUSD_BestAsk', 'close']].copy()
    
    # Verify row count after column selection
    print(f"After column selection: {selected_df.shape[0]} rows")
    assert selected_df.shape[0] == result_df.shape[0], "ERROR: Lost rows during column selection!"
    
    # Calculate the half spread
    selected_df['XRPUSD_HalfSpread'] = (selected_df['XRPUSD_BestAsk'] - selected_df['XRPUSD_BestBid']) / 2
    
    # Create a dictionary for renaming columns with proper capitalization
    rename_dict = {
        'open_time': 'Open_Time',
        'ID': 'ID',
        'DateTime': 'Date_Time',
        'Symbol': 'Symbol',
        'Quantity': 'Quantity',
        'Rate': 'Rate',
        'Side': 'Side',
        'XRPUSD_BestBid': 'XRPUSD_BestBid',
        'XRPUSD_BestAsk': 'XRPUSD_BestAsk',
        'XRPUSD_HalfSpread': 'HalfSpread',
        'close': 'Close'
    }
    
    # First rename the columns
    renamed_df = selected_df.rename(columns=rename_dict)
    
    # Then select the columns you want to keep in the final dataframe
    xrp_df = renamed_df[['Open_Time', 'ID', 'Date_Time', 'Symbol', 'Quantity', 
                        'Rate', 'Side', 'HalfSpread', 'Close']].copy()
    
    # Verify row count after renaming and selecting
    print(f"After column renaming: {xrp_df.shape[0]} rows")
    assert xrp_df.shape[0] == result_df.shape[0], "ERROR: Lost rows during column renaming!"
    
    # Create minute_xrp with calculations
    minute_xrp = xrp_df[['Open_Time', 'Symbol', 'Quantity', 'Rate', 'Side', 'HalfSpread', 'Close']].copy()
    minute_xrp['Weight'] = (minute_xrp['Quantity'] * minute_xrp['Rate'])
    minute_xrp = minute_xrp[['Open_Time', 'Symbol', 'Quantity', 'Weight', 'HalfSpread', 'Close']]
    
    # Verify row count after calculations
    print(f"After weight calculations: {minute_xrp.shape[0]} rows")
    assert minute_xrp.shape[0] == result_df.shape[0], "ERROR: Lost rows during weight calculation!"
    
    # Store the original transaction-level data before aggregating
    transaction_level_data = minute_xrp.copy()
    print(f"Transaction level data stored: {transaction_level_data.shape[0]} rows")
    
    # Make the timestamp field a datetime
    if not pd.api.types.is_datetime64_any_dtype(transaction_level_data['Open_Time']):
        transaction_level_data['Open_Time'] = pd.to_datetime(transaction_level_data['Open_Time'])
        transaction_level_data['Timestamp'] = transaction_level_data['Open_Time']  # Create Timestamp field for analysis
    
    # Now perform the aggregation with clear notification that rows will be intentionally consolidated
    print("\n*** INTENTIONAL DATA AGGREGATION BEGINS HERE ***")
    print(f"Before aggregation: {minute_xrp.shape[0]} rows")
    
    # Group by Open_Time and Symbol
    grouped_xrp = minute_xrp.groupby(['Open_Time', 'Symbol']).agg({
        'Quantity': 'sum',
        'Weight': 'sum',
        'HalfSpread': 'mean',
        'Close': ['mean', 'count']
    }).reset_index()
    
    # Flatten the multi-level column names
    grouped_xrp.columns = ['Open_Time', 'Symbol', 'Quantity', 'Sum_Weight', 
                        'Avg_HalfSpread', 'Avg_Close', 'Executions']
    
    # Make the timestamp field a datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(grouped_xrp['Open_Time']):
        grouped_xrp['Open_Time'] = pd.to_datetime(grouped_xrp['Open_Time'])
        grouped_xrp['Timestamp'] = grouped_xrp['Open_Time']  # Create Timestamp field for analysis
    
    print(f"After aggregation: {grouped_xrp.shape[0]} rows")
    print(f"Rows consolidated by aggregation: {minute_xrp.shape[0] - grouped_xrp.shape[0]}")
    
    # Calculate additional metrics with debugging
    print("Calculating metrics...")
    
    # Make sure there are no zero Quantity values that would cause division by zero
    zero_qty = (grouped_xrp['Quantity'] == 0).sum()
    if zero_qty > 0:
        print(f"WARNING: Found {zero_qty} rows with zero Quantity!")
        # Filter out rows with zero quantity to avoid division by zero
        grouped_xrp = grouped_xrp[grouped_xrp['Quantity'] > 0].copy()
        print(f"After filtering zero quantity rows: {grouped_xrp.shape[0]} rows")
    
    # Now calculate the metrics safely
    grouped_xrp['BitStamp_Bid'] = grouped_xrp['Sum_Weight'] / grouped_xrp['Quantity']
    grouped_xrp['Binance_Bid'] = grouped_xrp['Avg_Close'] - grouped_xrp['Avg_HalfSpread']
    
    # Check the calculated values
    print(f"BitStamp_Bid range: {grouped_xrp['BitStamp_Bid'].min()} to {grouped_xrp['BitStamp_Bid'].max()}")
    print(f"Binance_Bid range: {grouped_xrp['Binance_Bid'].min()} to {grouped_xrp['Binance_Bid'].max()}")
    
    # Create the executions dataframe with P&L calculation 
    xrp_executions = grouped_xrp[['Open_Time', 'Symbol', 'Quantity', 'Executions', 'BitStamp_Bid', 'Binance_Bid']].copy()
    
    # Calculate Min_PandL with debugs
    print("Calculating P&L...")
    print(f"BitStamp_Bid range: {xrp_executions['BitStamp_Bid'].min()} to {xrp_executions['BitStamp_Bid'].max()}")
    print(f"Binance_Bid range: {xrp_executions['Binance_Bid'].min()} to {xrp_executions['Binance_Bid'].max()}")
    print(f"Quantity range: {xrp_executions['Quantity'].min()} to {xrp_executions['Quantity'].max()}")
    
    # Check for NaN or zero values
    print(f"NaN in BitStamp_Bid: {xrp_executions['BitStamp_Bid'].isna().sum()}")
    print(f"NaN in Binance_Bid: {xrp_executions['Binance_Bid'].isna().sum()}")
    print(f"NaN in Quantity: {xrp_executions['Quantity'].isna().sum()}")
    
    # Calculate P&L with extra checks - CORRECTED CALCULATION
    # For buying at BitStamp (lower price) and selling at Binance (higher price)
    xrp_executions['Spread'] = xrp_executions['Binance_Bid'] - xrp_executions['BitStamp_Bid']
    print(f"Spread range: {xrp_executions['Spread'].min()} to {xrp_executions['Spread'].max()}")
    
    xrp_executions['Min_PandL'] = xrp_executions['Spread'] * xrp_executions['Quantity']
    print(f"Min_PandL range: {xrp_executions['Min_PandL'].min()} to {xrp_executions['Min_PandL'].max()}")
    
    # Create a complete time series with all minutes
    if not pd.api.types.is_datetime64_any_dtype(xrp_executions['Open_Time']):
        xrp_executions['Open_Time'] = pd.to_datetime(xrp_executions['Open_Time'])
    
    # Find the min and max timestamps to create a complete range
    min_time = xrp_executions['Open_Time'].min()
    max_time = xrp_executions['Open_Time'].max()
    
    # Create a complete minute range
    complete_minutes = pd.date_range(start=min_time, end=max_time, freq='1min')
    
    # Create a template dataframe with all minutes
    template_df = pd.DataFrame({
        'Open_Time': complete_minutes,
        'Symbol': 'XRPUSD'
    })
    
    # Use merge instead of update to ensure no data is lost
    xrp_complete_minutes = pd.merge(
        template_df, 
        xrp_executions, 
        on=['Open_Time', 'Symbol'], 
        how='left'
    )
    
    # Fill NaN values with defaults
    columns_to_fill = {
        'Quantity': 0.0,
        'Executions': 0,
        'BitStamp_Bid': 0.0,
        'Binance_Bid': 0.0,
        'Spread': 0.0,
        'Min_PandL': 0.0
    }
    
    # Only fill columns that exist
    fill_dict = {col: val for col, val in columns_to_fill.items() 
                if col in xrp_complete_minutes.columns}
    
    xrp_complete_minutes = xrp_complete_minutes.fillna(fill_dict)
    
    # Make sure we have all the rows we expect
    expected_rows = (max_time - min_time).total_seconds() // 60 + 1
    print(f"Complete minutes series has {xrp_complete_minutes.shape[0]} rows (expected {expected_rows})")
    
    # Create final dataset with cumulative values
    xrp_final = xrp_complete_minutes.copy()
    
    # Debug: Print available columns
    print(f"Available columns in xrp_final: {xrp_final.columns.tolist()}")
    
    # Now calculate cumulative sums
    xrp_final['Qty_C'] = xrp_final['Quantity'].cumsum()
    xrp_final['Exe_C'] = xrp_final['Executions'].cumsum()
    xrp_final['PandL_C'] = xrp_final['Min_PandL'].cumsum()
    
    # Return results as a dictionary with all dataframes for reference
    return {
        'raw_data': result_df,
        'transaction_level': transaction_level_data,
        'minute_aggregated': grouped_xrp,
        'final_pnl': xrp_final
    }