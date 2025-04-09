#!/usr/bin/env python
# coding: utf-8

"""
WhatsApp Report Generator for XRP Data Analysis
Functions to generate charts and save them for WhatsApp sharing
"""

import os
import json
import requests
import pandas as pd
import urllib.parse
from datetime import datetime
from chart_generator import generate_chart_url

def setup_whatsapp_report_directory():
    """
    Create a directory to store WhatsApp report images
    """
    # Create a timestamp for unique directory name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_dir = f"xrp_report_{timestamp}"
    
    # Create the directory
    os.makedirs(report_dir, exist_ok=True)
    
    print(f"WhatsApp report directory created: {report_dir}")
    return report_dir

def save_chart_image(chart_url, filepath):
    """
    Download a chart image from a URL and save it locally
    
    Args:
        chart_url (str): URL of the chart image
        filepath (str): Path where the image should be saved
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Download the image from the URL
        response = requests.get(chart_url, stream=True)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Save the image to the specified filepath
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(1024):
                    f.write(chunk)
            print(f"Chart saved to {filepath}")
            return True
        else:
            print(f"Error downloading chart: {response.status_code}")
            return False
    except Exception as e:
        print(f"Error saving chart image: {str(e)}")
        return False

def save_chart_with_description(chart_config, filepath, title, description):
    """
    Generate a chart, save it as an image, and create a description file
    
    Args:
        chart_config (dict): Chart configuration for QuickChart
        filepath (str): Path where the image should be saved (without extension)
        title (str): Title of the chart
        description (str): Description text for the chart
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Generate chart URL
    chart_url = generate_chart_url(chart_config)
    
    # Save the chart image
    image_path = f"{filepath}.png"
    if not save_chart_image(chart_url, image_path):
        return False
    
    # Save the description text
    desc_path = f"{filepath}_desc.txt"
    with open(desc_path, 'w') as f:
        f.write(f"{title}\n\n")
        f.write(f"{description}\n")
    
    print(f"Description saved to {desc_path}")
    return True

def create_pnl_chart(xrp_final, report_dir):
    """
    Create and save P&L chart for WhatsApp
    """
    # Make sure the Open_Time is a datetime type
    xrp_final['Open_Time'] = pd.to_datetime(xrp_final['Open_Time'])
    
    # Group by day to reduce data points
    xrp_final['Date'] = xrp_final['Open_Time'].dt.date
    daily_pnl = xrp_final.groupby('Date').agg({'PandL_C': 'last'}).reset_index()
    
    # Get the data in the right format for charting
    dates = [d.strftime('%Y-%m-%d') for d in daily_pnl['Date']]
    pnl_values = daily_pnl['PandL_C'].tolist()
    
    # Get key statistics
    final_pnl = pnl_values[-1]
    max_pnl = max(pnl_values)
    min_pnl = min(pnl_values)
    
    # Create chart configuration
    chart_config = {
        "type": "line",
        "data": {
            "labels": dates,
            "datasets": [
                {
                    "label": "Cumulative P&L",
                    "data": pnl_values,
                    "borderColor": "#1E88E5",
                    "backgroundColor": "rgba(30, 136, 229, 0.2)",
                    "fill": "origin",
                    "tension": 0.1
                }
            ]
        },
        "options": {
            "plugins": {
                "title": {
                    "display": True,
                    "text": "XRP Trading Cumulative Profit & Loss Over Time",
                    "font": {
                        "size": 16,
                        "weight": "bold"
                    }
                },
                "subtitle": {
                    "display": True,
                    "text": f"Final P&L: ${final_pnl:.2f} | Max: ${max_pnl:.2f} | Min: ${min_pnl:.2f}",
                    "font": {
                        "size": 12
                    }
                }
            },
            "scales": {
                "y": {
                    "ticks": {
                        "callback": "function(value) { return '$' + value.toFixed(2); }"
                    },
                    "title": {
                        "display": True,
                        "text": "Cumulative P&L ($)"
                    }
                },
                "x": {
                    "title": {
                        "display": True,
                        "text": "Date"
                    }
                }
            }
        }
    }
    
    # Create description for WhatsApp
    description = (
        f"This chart shows the cumulative Profit & Loss over time for XRP trading.\n"
        f"• Final P&L: ${final_pnl:.2f}\n"
        f"• Maximum P&L: ${max_pnl:.2f}\n"
        f"• Minimum P&L: ${min_pnl:.2f}\n"
        f"• Analysis period: {dates[0]} to {dates[-1]}"
    )
    
    # Save the chart and description
    filepath = os.path.join(report_dir, "1_pnl_chart")
    return save_chart_with_description(chart_config, filepath, "XRP Cumulative P&L", description)

def create_quantity_chart(xrp_final, report_dir):
    """
    Create and save Quantity chart for WhatsApp
    """
    # Make sure the Open_Time is a datetime type
    xrp_final['Open_Time'] = pd.to_datetime(xrp_final['Open_Time'])
    
    # Group by day to reduce data points
    xrp_final['Date'] = xrp_final['Open_Time'].dt.date
    daily_qty = xrp_final.groupby('Date').agg({'Qty_C': 'last'}).reset_index()
    
    # Get the data in the right format for charting
    dates = [d.strftime('%Y-%m-%d') for d in daily_qty['Date']]
    qty_values = daily_qty['Qty_C'].tolist()
    
    # Get key statistics
    final_qty = qty_values[-1]
    
    # Create chart configuration
    chart_config = {
        "type": "line",
        "data": {
            "labels": dates,
            "datasets": [
                {
                    "label": "Cumulative Quantity",
                    "data": qty_values,
                    "borderColor": "#6A1B9A",
                    "backgroundColor": "rgba(106, 27, 154, 0.2)",
                    "fill": "origin",
                    "tension": 0.1
                }
            ]
        },
        "options": {
            "plugins": {
                "title": {
                    "display": True,
                    "text": "XRP Trading Cumulative Quantity Over Time",
                    "font": {
                        "size": 16,
                        "weight": "bold"
                    }
                },
                "subtitle": {
                    "display": True,
                    "text": f"Total Quantity: {final_qty:,.2f} XRP",
                    "font": {
                        "size": 12
                    }
                }
            },
            "scales": {
                "y": {
                    "ticks": {
                        "callback": "function(value) { return value.toLocaleString(); }"
                    },
                    "title": {
                        "display": True,
                        "text": "Cumulative Quantity"
                    }
                },
                "x": {
                    "title": {
                        "display": True,
                        "text": "Date"
                    }
                }
            }
        }
    }
    
    # Create description for WhatsApp
    description = (
        f"This chart shows the cumulative quantity of XRP traded over time.\n"
        f"• Total Quantity: {final_qty:,.2f} XRP\n"
        f"• Analysis period: {dates[0]} to {dates[-1]}"
    )
    
    # Save the chart and description
    filepath = os.path.join(report_dir, "2_quantity_chart")
    return save_chart_with_description(chart_config, filepath, "XRP Cumulative Quantity", description)

def create_executions_chart(xrp_final, report_dir):
    """
    Create and save Executions chart for WhatsApp
    """
    # Make sure the Open_Time is a datetime type
    xrp_final['Open_Time'] = pd.to_datetime(xrp_final['Open_Time'])
    
    # Group by day to reduce data points
    xrp_final['Date'] = xrp_final['Open_Time'].dt.date
    daily_exe = xrp_final.groupby('Date').agg({'Exe_C': 'last'}).reset_index()
    
    # Get the data in the right format for charting
    dates = [d.strftime('%Y-%m-%d') for d in daily_exe['Date']]
    exe_values = daily_exe['Exe_C'].tolist()
    
    # Get key statistics
    final_exe = exe_values[-1]
    days_elapsed = len(dates)
    avg_exe_per_day = final_exe / days_elapsed if days_elapsed > 0 else 0
    
    # Create chart configuration
    chart_config = {
        "type": "line",
        "data": {
            "labels": dates,
            "datasets": [
                {
                    "label": "Cumulative Executions",
                    "data": exe_values,
                    "borderColor": "#FF5722",
                    "backgroundColor": "rgba(255, 87, 34, 0.2)",
                    "fill": "origin",
                    "tension": 0.1
                }
            ]
        },
        "options": {
            "plugins": {
                "title": {
                    "display": True,
                    "text": "XRP Trading Cumulative Executions Over Time",
                    "font": {
                        "size": 16,
                        "weight": "bold"
                    }
                },
                "subtitle": {
                    "display": True,
                    "text": f"Total Executions: {int(final_exe):,} | Avg: {int(avg_exe_per_day):,} per day",
                    "font": {
                        "size": 12
                    }
                }
            },
            "scales": {
                "y": {
                    "ticks": {
                        "callback": "function(value) { return value.toLocaleString(); }"
                    },
                    "title": {
                        "display": True,
                        "text": "Cumulative Executions"
                    }
                },
                "x": {
                    "title": {
                        "display": True,
                        "text": "Date"
                    }
                }
            }
        }
    }
    
    # Create description for WhatsApp
    description = (
        f"This chart shows the cumulative number of XRP trade executions over time.\n"
        f"• Total Executions: {int(final_exe):,}\n"
        f"• Average: {int(avg_exe_per_day):,} executions per day\n"
        f"• Analysis period: {dates[0]} to {dates[-1]}"
    )
    
    # Save the chart and description
    filepath = os.path.join(report_dir, "3_executions_chart")
    return save_chart_with_description(chart_config, filepath, "XRP Cumulative Executions", description)

def create_pnl_distribution_chart(minute_data, report_dir):
    """
    Create and save P&L Distribution chart for WhatsApp
    """
    # Check if required columns exist and create them if needed
    if 'Min_PandL' not in minute_data.columns:
        # Calculate Min_PandL if it doesn't exist
        if 'Spread' not in minute_data.columns and 'Binance_Bid' in minute_data.columns and 'BitStamp_Bid' in minute_data.columns:
            minute_data['Spread'] = minute_data['Binance_Bid'] - minute_data['BitStamp_Bid']
            
        # Check if we have Spread and Quantity to calculate Min_PandL
        if 'Spread' in minute_data.columns and 'Quantity' in minute_data.columns:
            minute_data['Min_PandL'] = minute_data['Spread'] * minute_data['Quantity']
        else:
            # If we can't calculate Min_PandL, we can't continue
            print("Error: Cannot calculate P&L distribution. Required columns not available.")
            print(f"Available columns: {minute_data.columns.tolist()}")
            return False
    
    # Calculate P&L as percentage of trade value if not already present
    if 'Trade_Value' not in minute_data.columns and 'BitStamp_Bid' in minute_data.columns:
        minute_data['Trade_Value'] = minute_data['Quantity'] * minute_data['BitStamp_Bid']
    
    if 'PnL_Percent' not in minute_data.columns:
        minute_data['PnL_Percent'] = (minute_data['Min_PandL'] / minute_data['Trade_Value']) * 100
    
    # Filter out minutes with zero quantity or zero trade value
    minute_data = minute_data[(minute_data['Quantity'] > 0) & (minute_data['Trade_Value'] > 0)].copy()
    
    # Calculate statistics
    pnl_min = minute_data['PnL_Percent'].min()
    pnl_max = minute_data['PnL_Percent'].max()
    pnl_mean = minute_data['PnL_Percent'].mean()
    pnl_median = minute_data['PnL_Percent'].median()
    pnl_std = minute_data['PnL_Percent'].std()
    profit_count = (minute_data['PnL_Percent'] > 0).sum()
    profit_percent = (profit_count / len(minute_data)) * 100 if len(minute_data) > 0 else 0
    
    # Create histogram bins for distribution
    bins = 15
    
    # We need to bin the data manually since we'll create a bar chart
    hist_data = pd.cut(minute_data['PnL_Percent'], bins=bins).value_counts().sort_index()
    
    # Get bin labels and counts
    bin_labels = [f"{interval.left:.2f}" for interval in hist_data.index]
    counts = hist_data.values.tolist()
    
    # Create chart configuration
    chart_config = {
        "type": "bar",
        "data": {
            "labels": bin_labels,
            "datasets": [
                {
                    "label": "Frequency",
                    "data": counts,
                    "backgroundColor": "rgba(63, 81, 181, 0.6)",
                    "borderColor": "rgb(63, 81, 181)",
                    "borderWidth": 1
                }
            ]
        },
        "options": {
            "plugins": {
                "title": {
                    "display": True,
                    "text": "Distribution of % P&L per Minute",
                    "font": {
                        "size": 16,
                        "weight": "bold"
                    }
                },
                "subtitle": {
                    "display": True,
                    "text": f"Mean: {pnl_mean:.4f}% | Median: {pnl_median:.4f}% | Std Dev: {pnl_std:.4f}%",
                    "font": {
                        "size": 12
                    }
                }
            },
            "scales": {
                "y": {
                    "title": {
                        "display": True,
                        "text": "Frequency"
                    }
                },
                "x": {
                    "title": {
                        "display": True,
                        "text": "P&L Percent (%)"
                    }
                }
            }
        }
    }
    
    # Create description for WhatsApp
    description = (
        f"This chart shows the distribution of P&L percentages per minute.\n"
        f"• Minutes with trades: {len(minute_data)}\n"
        f"• Mean: {pnl_mean:.4f}%\n"
        f"• Median: {pnl_median:.4f}%\n"
        f"• Profitable minutes: {profit_count} ({profit_percent:.1f}%)\n"
        f"• Range: {pnl_min:.4f}% to {pnl_max:.4f}%"
    )
    
    # Save the chart and description
    filepath = os.path.join(report_dir, "4_pnl_distribution_chart")
    return save_chart_with_description(chart_config, filepath, "XRP P&L Distribution", description)

def create_daily_distribution_chart(results, report_dir):
    """
    Create and save Daily P&L Distribution chart for WhatsApp
    """
    # Get the minute-level data
    minute_data = results['minute_aggregated'].copy()
    
    # Filter out minutes with zero quantity
    minute_data = minute_data[minute_data['Quantity'] > 0].copy()
    
    # Ensure we have Open_Time as datetime
    if not pd.api.types.is_datetime64_any_dtype(minute_data['Open_Time']):
        minute_data['Open_Time'] = pd.to_datetime(minute_data['Open_Time'])
    
    # Add date column for grouping
    minute_data['Date'] = minute_data['Open_Time'].dt.date
    
    # Check if required columns exist and create them if needed
    if 'Min_PandL' not in minute_data.columns:
        # Calculate Min_PandL if it doesn't exist
        if 'Spread' not in minute_data.columns and 'Binance_Bid' in minute_data.columns and 'BitStamp_Bid' in minute_data.columns:
            minute_data['Spread'] = minute_data['Binance_Bid'] - minute_data['BitStamp_Bid']
            
        # Check if we have Spread and Quantity to calculate Min_PandL
        if 'Spread' in minute_data.columns and 'Quantity' in minute_data.columns:
            minute_data['Min_PandL'] = minute_data['Spread'] * minute_data['Quantity']
        else:
            # If we can't calculate Min_PandL, we can't continue
            print("Error: Cannot calculate daily distribution. Required columns not available.")
            print(f"Available columns: {minute_data.columns.tolist()}")
            return False
    
    # Calculate P&L as percentage of trade value if not already present
    if 'Trade_Value' not in minute_data.columns and 'BitStamp_Bid' in minute_data.columns:
        minute_data['Trade_Value'] = minute_data['Quantity'] * minute_data['BitStamp_Bid']
    
    if 'PnL_Percent' not in minute_data.columns:
        minute_data['PnL_Percent'] = (minute_data['Min_PandL'] / minute_data['Trade_Value']) * 100
    
    # Group by date for daily stats - Simple aggregation
    daily_stats = minute_data.groupby('Date').agg({
        'Min_PandL': 'sum',
        'Trade_Value': 'sum'
    }).reset_index()
    
    # Calculate overall P&L percent for each day
    daily_stats['DailyPnLPercent'] = (daily_stats['Min_PandL'] / daily_stats['Trade_Value']) * 100
    
    # Sort by date
    daily_stats = daily_stats.sort_values('Date')
    
    # Prepare data for the chart
    dates = [d.strftime('%Y-%m-%d') for d in daily_stats['Date']]
    daily_pnl = daily_stats['DailyPnLPercent'].tolist()
    
    # Calculate overall statistics
    overall_min = daily_stats['DailyPnLPercent'].min()
    overall_max = daily_stats['DailyPnLPercent'].max()
    overall_mean = daily_stats['DailyPnLPercent'].mean()
    overall_median = daily_stats['DailyPnLPercent'].median()
    
    # Count profitable days
    profitable_days = (daily_stats['DailyPnLPercent'] > 0).sum()
    days_percentage = (profitable_days / len(daily_stats)) * 100 if len(daily_stats) > 0 else 0
    
    # Create a bar chart config showing daily P&L
    chart_config = {
        "type": "bar",
        "data": {
            "labels": dates,
            "datasets": [
                {
                    "label": "Daily P&L %",
                    "data": daily_pnl,
                    "backgroundColor": "rgba(75, 192, 192, 0.7)",
                    "borderColor": "rgba(75, 192, 192, 1)",
                    "borderWidth": 1
                }
            ]
        },
        "options": {
            "plugins": {
                "title": {
                    "display": True,
                    "text": "Daily P&L Distribution Over Time",
                    "font": {
                        "size": 16,
                        "weight": "bold"
                    }
                },
                "subtitle": {
                    "display": True,
                    "text": f"Mean: {overall_mean:.4f}% | Median: {overall_median:.4f}% | Profitable Days: {profitable_days}/{len(daily_stats)} ({days_percentage:.1f}%)",
                    "font": {
                        "size": 12
                    }
                }
            },
            "scales": {
                "y": {
                    "title": {
                        "display": True,
                        "text": "P&L Percent (%)"
                    },
                    "grid": {
                        "display": True
                    }
                },
                "x": {
                    "title": {
                        "display": True,
                        "text": "Date"
                    }
                }
            }
        }
    }
    
    # Create description for WhatsApp
    description = (
        f"This chart shows the daily P&L distribution over time for XRP trading.\n"
        f"• Trading days: {len(daily_stats)}\n"
        f"• Profitable days: {profitable_days} ({days_percentage:.1f}%)\n"
        f"• Best day: {overall_max:.4f}%\n"
        f"• Worst day: {overall_min:.4f}%\n"
        f"• Analysis period: {dates[0]} to {dates[-1]}"
    )
    
    # Save the chart and description
    filepath = os.path.join(report_dir, "5_daily_distribution_chart")
    return save_chart_with_description(chart_config, filepath, "XRP Daily P&L Distribution Over Time", description)

def create_whatsapp_report(results):
    """
    Create a complete WhatsApp report with all charts
    
    Args:
        results (dict): Dictionary containing all the processed XRP data
        
    Returns:
        str: Path to the report directory
    """
    # Create report directory
    report_dir = setup_whatsapp_report_directory()
    
    # Generate all charts
    print("\nGenerating charts for WhatsApp report...")
    
    # 1. P&L Chart
    print("Creating P&L chart...")
    create_pnl_chart(results['final_pnl'], report_dir)
    
    # 2. Quantity Chart
    print("Creating Quantity chart...")
    create_quantity_chart(results['final_pnl'], report_dir)
    
    # 3. Executions Chart
    print("Creating Executions chart...")
    create_executions_chart(results['final_pnl'], report_dir)
    
    # 4. P&L Distribution Chart
    print("Creating P&L Distribution chart...")
    create_pnl_distribution_chart(results['minute_aggregated'], report_dir)
    
    # 5. Daily Distribution Chart
    print("Creating Daily Distribution chart...")
    create_daily_distribution_chart(results, report_dir)
    
    # Create a summary file
    summary_path = os.path.join(report_dir, "summary.txt")
    with open(summary_path, 'w') as f:
        f.write(f"XRP Trading Report - Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"This report contains 5 charts analyzing XRP trading performance:\n\n")
        f.write(f"1. XRP Cumulative P&L - Shows profit and loss over time\n")
        f.write(f"2. XRP Cumulative Quantity - Shows total XRP traded\n")
        f.write(f"3. XRP Cumulative Executions - Shows number of trades\n")
        f.write(f"4. XRP P&L Distribution - Shows distribution of profits\n")
        f.write(f"5. XRP Daily P&L Distribution - Shows daily performance\n\n")
        
        # Add overall statistics
        if 'PandL_C' in results['final_pnl'].columns:
            pnl_c_series = results['final_pnl']['PandL_C']
            f.write(f"Overall P&L Statistics:\n")
            f.write(f"- Min: ${pnl_c_series.min():.2f}\n")
            f.write(f"- Max: ${pnl_c_series.max():.2f}\n")
            f.write(f"- Final: ${pnl_c_series.iloc[-1]:.2f}\n\n")
        
        f.write(f"To share this report via WhatsApp:\n")
        f.write(f"1. Send each PNG image file\n")
        f.write(f"2. Copy and paste the corresponding text from each _desc.txt file\n")
    
    print(f"\nWhatsApp report generated successfully in: {report_dir}")
    return report_dir