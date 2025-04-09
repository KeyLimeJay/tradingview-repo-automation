# XRP Data Collector

This project automatically collects minute-level XRP/USDT trading data from Binance and maintains a continuously updated dataset. It focuses solely on efficient data collection without visualization.

## Features

- Automatically downloads new XRP/USDT price data daily
- Detects and retrieves missing data for up to 7 days back
- Robust retry logic for handling network issues and API limitations
- Maintains a consolidated historical dataset 
- Optional daily file storage
- Comprehensive logging for monitoring the collection process
- Can be scheduled to run automatically with minimal maintenance

## Folder Structure

```
xrp_data_collector/
├── auto_xrp_collector.py        # Main script for automatic data collection
├── README.md                    # Documentation
├── data/                        # Data storage directory
│   └── xrp_binance/             # XRP data from Binance
│       ├── xrp_binance_minute_all.csv       # Combined data file
│       ├── xrp_binance_minute_20250326.csv  # Daily data files
│       └── ...
├── logs/                        # Log files directory
│   ├── xrp_collector_20250402.log
│   └── ...
└── scripts/                     # Helper scripts
    └── schedule_collector.sh    # Script to schedule data collection
```

## Setup Instructions

### Prerequisites

The script requires the following Python libraries:
- pandas
- numpy
- requests

### Installation

1. Run the setup script to create the directory structure and files
2. Make the scripts executable:
   ```bash
   chmod +x auto_xrp_collector.py
   chmod +x scripts/schedule_collector.sh
   ```

### Configuration

You can customize the data collection by modifying the `BINANCE_CONFIG` dictionary in `auto_xrp_collector.py`:

- `data_config`:
  - `symbol`: Change the trading pair (default: "XRPUSDT")
  - `interval`: Adjust time interval (options: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1mo)
  - `market_type`: Change market type (options: spot, futures/um, futures/cm)

- `retrieval`:
  - `days_to_check`: Number of days to look back for missing data (default: 7)
  - `sleep_time`: Delay between requests to avoid rate limiting (default: 1.0)
  - `max_retries`: Maximum retry attempts for failed downloads (default: 3)

- `storage`:
  - `save_path`: Directory path for storing data files
  - `file_format`: File format for saving data (default: "csv")
  - `combined_file`: Filename for the combined historical dataset
  - `save_daily_files`: Whether to save individual daily files (set to False to save space)

### Running Manually

To run the collector manually:

```bash
python auto_xrp_collector.py
```

### Setting Up Automatic Scheduling

To set up automatic daily collection:

```bash
./scripts/schedule_collector.sh
```

This will create a cron job that runs the collector daily at 1:00 AM.

## Data Format

The collected data includes the following columns:

### Raw Data Columns (from Binance)
- `open_time`: Timestamp for the opening of the candle
- `open`: Opening price
- `high`: Highest price during the period
- `low`: Lowest price during the period
- `close`: Closing price
- `volume`: Trading volume
- `close_time`: Timestamp for the closing of the candle
- `quote_volume`: Trading volume in quote currency (USDT)
- `count`: Number of trades
- `taker_buy_volume`: Volume of taker buy trades
- `taker_buy_quote_volume`: Volume of taker buy trades in quote currency
- `ignore`: Unused column from Binance

### Derived Columns
- `date`: Calendar date extracted from open_time (for easier filtering)

The timestamps are converted to proper datetime objects and numeric columns are converted to proper numeric types for easy use in analysis.

## Troubleshooting

Check the log files in the `logs` directory for detailed information about each run.

Common issues:
- **No data available**: Binance may delay publishing the data. The script will catch up on the next run.
- **Network errors**: Temporary network issues will be logged, and the script will retry missing dates on the next run.
- **Permission errors**: Make sure the script has write access to the data and logs directories.
