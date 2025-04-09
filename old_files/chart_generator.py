#!/usr/bin/env python
# coding: utf-8

"""
Chart Generator Module for XRP Data Analysis
Functions to generate and post charts to Slack
"""

import json
import requests
import pandas as pd
import urllib.parse
from datetime import datetime

# Slack webhook URL for posting charts
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/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"

def generate_chart_url(chart_config):
    """
    Generate a chart URL using the QuickChart API
    """
    # Convert the chart configuration to a URL-encoded JSON string
    chart_config_str = json.dumps(chart_config)
    encoded_config = urllib.parse.quote(chart_config_str)
    
    # Generate the chart URL
    chart_url = f"https://quickchart.io/chart?c={encoded_config}&width=800&height=400"
    
    print(f"Chart URL generated with length: {len(chart_url)}")
    return chart_url

def post_to_slack(chart_url, title, description):
    """
    Post a chart to Slack
    """
    # Create the message payload
    message = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{title}*"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": description
                }
            },
            {
                "type": "image",
                "title": {
                    "type": "plain_text",
                    "text": title
                },
                "image_url": chart_url,
                "alt_text": title
            }
        ]
    }
    
    # Post the message to Slack
    response = requests.post(
        SLACK_WEBHOOK_URL,
        data=json.dumps(message),
        headers={'Content-Type': 'application/json'}
    )
    
    # Check for errors
    if response.status_code != 200:
        print(f"Error posting to Slack: {response.status_code} - {response.text}")
        return False
    
    print(f"Chart '{title}' posted to Slack successfully!")
    return True

def post_chart_pnl(xrp_final):
    """
    Create and post a P&L chart to Slack
    """
    # Make sure the Open_Time is a datetime type
    xrp_final['Open_Time'] = pd.to_datetime(xrp_final['Open_Time'])
    
    # Group by day to reduce data points (QuickChart has URL length limits)
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
    
    # Generate chart URL
    chart_url = generate_chart_url(chart_config)
    
    # Create description for Slack
    description = (
        f"This chart shows the cumulative Profit & Loss over time for XRP trading.\n"
        f"• Final P&L: ${final_pnl:.2f}\n"
        f"• Maximum P&L: ${max_pnl:.2f}\n"
        f"• Minimum P&L: ${min_pnl:.2f}\n"
        f"• Analysis period: {dates[0]} to {dates[-1]}"
    )
    
    # Post to Slack
    return post_to_slack(chart_url, "XRP Cumulative P&L", description)

def post_chart_quantity(xrp_final):
    """
    Create and post a Quantity chart to Slack
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
    
    # Generate chart URL
    chart_url = generate_chart_url(chart_config)
    
    # Create description for Slack
    description = (
        f"This chart shows the cumulative quantity of XRP traded over time.\n"
        f"• Total Quantity: {final_qty:,.2f} XRP\n"
        f"• Analysis period: {dates[0]} to {dates[-1]}"
    )
    
    # Post to Slack
    return post_to_slack(chart_url, "XRP Cumulative Quantity", description)

def post_chart_executions(xrp_final):
    """
    Create and post an Executions chart to Slack
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
    
    # Generate chart URL
    chart_url = generate_chart_url(chart_config)
    
    # Create description for Slack
    description = (
        f"This chart shows the cumulative number of XRP trade executions over time.\n"
        f"• Total Executions: {int(final_exe):,}\n"
        f"• Average: {int(avg_exe_per_day):,} executions per day\n"
        f"• Analysis period: {dates[0]} to {dates[-1]}"
    )
    
    # Post to Slack
    return post_to_slack(chart_url, "XRP Cumulative Executions", description)

def post_chart_pnl_distribution(minute_data):
    """
    Create and post a P&L percentage distribution chart to Slack
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
    
    # Generate chart URL
    chart_url = generate_chart_url(chart_config)
    
    # Create description for Slack
    description = (
        f"This chart shows the distribution of P&L percentages per minute.\n"
        f"• Minutes with trades: {len(minute_data)}\n"
        f"• Mean: {pnl_mean:.4f}%\n"
        f"• Median: {pnl_median:.4f}%\n"
        f"• Profitable minutes: {profit_count} ({profit_percent:.1f}%)\n"
        f"• Range: {pnl_min:.4f}% to {pnl_max:.4f}%"
    )
    
    # Post to Slack
    return post_to_slack(chart_url, "XRP P&L Distribution", description)

def post_chart_hourly_performance(results):
    """
    Create and post an hourly P&L performance chart to Slack
    """
    # Get the minute-level data
    minute_data = results['minute_aggregated'].copy()
    
    # Filter out minutes with zero quantity
    minute_data = minute_data[minute_data['Quantity'] > 0].copy()
    
    # Ensure we have Open_Time as datetime
    if not pd.api.types.is_datetime64_any_dtype(minute_data['Open_Time']):
        minute_data['Open_Time'] = pd.to_datetime(minute_data['Open_Time'])
    
    # Check if we need to create Min_PandL column
    if 'Min_PandL' not in minute_data.columns:
        # Calculate Min_PandL if it doesn't exist
        if 'Spread' not in minute_data.columns and 'Binance_Bid' in minute_data.columns and 'BitStamp_Bid' in minute_data.columns:
            minute_data['Spread'] = minute_data['Binance_Bid'] - minute_data['BitStamp_Bid']
            
        # Check if we have Spread and Quantity to calculate Min_PandL
        if 'Spread' in minute_data.columns and 'Quantity' in minute_data.columns:
            minute_data['Min_PandL'] = minute_data['Spread'] * minute_data['Quantity']
        else:
            # If we can't calculate Min_PandL, we can't continue
            print("Error: Cannot calculate hourly performance. Required columns not available.")
            print(f"Available columns: {minute_data.columns.tolist()}")
            return False
    
    # Create hourly intervals
    minute_data['Hour'] = minute_data['Open_Time'].dt.floor('H')
    
    # Group by hourly intervals
    hourly_data = minute_data.groupby('Hour').agg({
        'Min_PandL': 'sum',
        'Quantity': 'sum',
        'BitStamp_Bid': 'mean',
        'Binance_Bid': 'mean',
        'Executions': 'sum'
    }).reset_index()
    
    # Calculate trade value
    hourly_data['Trade_Value'] = hourly_data['Quantity'] * hourly_data['BitStamp_Bid']
    
    # Calculate P&L percentage
    hourly_data['PnL_Percent'] = (hourly_data['Min_PandL'] / hourly_data['Trade_Value']) * 100
    
    # Format hours for chart labels
    hour_labels = hourly_data['Hour'].dt.strftime('%Y-%m-%d %H:%M').tolist()
    
    # Limit the number of data points if too many
    if len(hour_labels) > 30:
        # Sample every Nth point
        n = len(hour_labels) // 30 + 1
        hour_labels = hour_labels[::n]
        pnl_percent = hourly_data['PnL_Percent'].tolist()[::n]
        pnl_values = hourly_data['Min_PandL'].tolist()[::n]
    else:
        pnl_percent = hourly_data['PnL_Percent'].tolist()
        pnl_values = hourly_data['Min_PandL'].tolist()
    
    # Calculate statistics
    pnl_mean = hourly_data['PnL_Percent'].mean()
    pnl_std = hourly_data['PnL_Percent'].std()
    upper_limit = pnl_mean + 3 * pnl_std
    lower_limit = pnl_mean - 3 * pnl_std
    
    # Create chart configuration with simple colors
    chart_config = {
        "type": "bar",
        "data": {
            "labels": hour_labels,
            "datasets": [
                {
                    "type": "line",
                    "label": "P&L Percent",
                    "data": pnl_percent,
                    "borderColor": "#3F51B5",
                    "backgroundColor": "rgba(63, 81, 181, 0.2)",
                    "yAxisID": "y",
                    "fill": False,
                    "tension": 0.1
                },
                {
                    "type": "bar",
                    "label": "P&L Value",
                    "data": pnl_values,
                    "backgroundColor": "rgba(76, 175, 80, 0.7)",
                    "yAxisID": "y1"
                }
            ]
        },
        "options": {
            "plugins": {
                "title": {
                    "display": True,
                    "text": "Hourly P&L Performance",
                    "font": {
                        "size": 16,
                        "weight": "bold"
                    }
                },
                "subtitle": {
                    "display": True,
                    "text": f"Mean: {pnl_mean:.4f}% | Upper Control: {upper_limit:.4f}% | Lower Control: {lower_limit:.4f}%",
                    "font": {
                        "size": 12
                    }
                }
            },
            "scales": {
                "y": {
                    "position": "left",
                    "title": {
                        "display": True,
                        "text": "P&L Percent (%)"
                    }
                },
                "y1": {
                    "position": "right",
                    "title": {
                        "display": True,
                        "text": "P&L Value ($)"
                    },
                    "grid": {
                        "drawOnChartArea": False
                    }
                },
                "x": {
                    "title": {
                        "display": True,
                        "text": "Hour"
                    }
                }
            }
        }
    }
    
    # Generate chart URL
    chart_url = generate_chart_url(chart_config)
    
    # Calculate overall performance metrics
    total_profit = hourly_data['Min_PandL'].sum()
    total_traded = hourly_data['Trade_Value'].sum()
    overall_percent = (total_profit / total_traded) * 100 if total_traded > 0 else 0
    
    # Create description for Slack
    description = (
        f"This chart shows the hourly P&L performance for XRP trading.\n"
        f"• Total Profit/Loss: ${total_profit:.2f}\n"
        f"• Total Value Traded: ${total_traded:.2f}\n"
        f"• Overall P&L: {overall_percent:.4f}%\n"
        f"• Hours with Trading: {len(hourly_data)}\n"
        f"• Timespan: {hourly_data['Hour'].min().strftime('%Y-%m-%d %H:%M')} to {hourly_data['Hour'].max().strftime('%Y-%m-%d %H:%M')}"
    )
    
    # Post to Slack
    return post_to_slack(chart_url, "XRP Hourly Performance", description)

def post_chart_daily_distribution(results):
    """
    Create and post a daily P&L distribution chart to Slack showing trends over time
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
    
    # Create a simpler bar chart config
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
    
    # Generate chart URL
    chart_url = generate_chart_url(chart_config)
    
    # Create description for Slack
    description = (
        f"This chart shows the daily P&L distribution over time for XRP trading.\n"
        f"• Trading days: {len(daily_stats)}\n"
        f"• Profitable days: {profitable_days} ({days_percentage:.1f}%)\n"
        f"• Best day: {overall_max:.4f}%\n"
        f"• Worst day: {overall_min:.4f}%\n"
        f"• Analysis period: {dates[0]} to {dates[-1]}"
    )
    
    # Post the chart to Slack
    return post_to_slack(chart_url, "XRP Daily P&L Distribution Over Time", description)