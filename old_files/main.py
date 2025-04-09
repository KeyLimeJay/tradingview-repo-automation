#!/usr/bin/env python
# coding: utf-8

"""
XRP Analysis and Slack Chart Generator
This script analyzes XRP trading data and generates charts for posting to Slack.
"""

import os
import pandas as pd
from data_processor import process_xrp_data
from chart_generator import (
    post_chart_pnl, 
    post_chart_quantity, 
    post_chart_executions, 
    post_chart_pnl_distribution,
    post_chart_hourly_performance,
    post_chart_daily_distribution
)
from whatsapp_report import create_whatsapp_report

# File path for the XRP data
CSV_FILE_PATH = "/opt/otcxn/tradingview-repo-automation/xrp_04_matches/output/xrp_matched.csv"

def load_csv_file(file_path):
    """
    Load CSV file directly from the server
    """
    try:
        print(f"Loading data from {file_path}...")
        df = pd.read_csv(file_path)
        
        print(f"Successfully loaded CSV file")
        print(f"Initial DataFrame shape: {df.shape}")
        
        # Store original row count for verification
        original_rows = df.shape[0]
        
        # Drop columns with null values instead of rows
        df = df.dropna(axis=1)
        print(f"After removing columns with nulls, DataFrame shape: {df.shape}")
        
        # Verify no rows were lost
        assert df.shape[0] == original_rows, "ERROR: Row count changed after dropping columns!"
        
        return df
            
    except Exception as e:
        print(f"Error loading CSV file: {str(e)}")
        return None

def main():
    """Main entry point for the XRP analysis and chart generation"""
    # Load the CSV file
    print("Step 1: Loading CSV file...")
    result_df = load_csv_file(CSV_FILE_PATH)
    
    if result_df is not None:
        # Set display options to show all columns
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        
        print(f"\nStep 2: Processing data (with {result_df.shape[0]} rows)...")
        results = process_xrp_data(result_df)
        
        # Display sample rows from each stage
        print("\nOriginal data sample (first 5 rows):")
        print(results['raw_data'].head())
        
        print("\nTransaction level data sample (first 5 rows):")
        print(results['transaction_level'].head())
        
        print("\nAggregated data sample (first 5 rows):")
        print(results['minute_aggregated'].head())
        
        print("\nFinal P&L data sample (first 5 rows):")
        print(results['final_pnl'].head())
        
        # Show P&L stats
        print("\nP&L Statistics:")
        if 'Min_PandL' in results['minute_aggregated'].columns:
            pnl_series = results['minute_aggregated']['Min_PandL']
            print(f"Individual P&L values - Min: {pnl_series.min()}, Max: {pnl_series.max()}, Sum: {pnl_series.sum()}")
        
        if 'PandL_C' in results['final_pnl'].columns:
            pnl_c_series = results['final_pnl']['PandL_C']
            print(f"Cumulative P&L - Min: {pnl_c_series.min()}, Max: {pnl_c_series.max()}, Final: {pnl_c_series.iloc[-1]}")
        
        print("\nData shapes:")
        print(f"Original data: {results['raw_data'].shape}")
        print(f"Transaction level: {results['transaction_level'].shape}")
        print(f"Minute aggregated: {results['minute_aggregated'].shape}")
        print(f"Final P&L: {results['final_pnl'].shape}")
        
        print("\nStep 3: Generating and posting charts to Slack...")
        
        # Generate and post each chart to Slack
        post_chart_pnl(results['final_pnl'])
        post_chart_quantity(results['final_pnl'])
        post_chart_executions(results['final_pnl'])
        post_chart_pnl_distribution(results['minute_aggregated'])
        post_chart_hourly_performance(results)
        post_chart_daily_distribution(results)
        
        print("\nAll charts have been generated and posted to Slack!")
        
        # Generate WhatsApp report
        print("\nStep 4: Generating WhatsApp report...")
        whatsapp_report_dir = create_whatsapp_report(results)
        print(f"WhatsApp report created in: {whatsapp_report_dir}")
    else:
        print("Failed to load CSV file.")

if __name__ == "__main__":
    main()