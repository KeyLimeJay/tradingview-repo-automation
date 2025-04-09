# Trade Execution Processor

This application processes trade execution data files, filters based on configurable criteria, and generates reports with specified columns.

## Features

- Processes trade data from CSV files
- Configurable filters for symbols, sides, and more
- Support for selecting top rate pairs
- Handles duplicate IDs
- Configurable output columns
- Automated scheduling

## Configuration

All settings are managed through the `config.json` file:

### Data Source
```json
"data_source": {
  "path": "/opt/otcxn/tradingview-repo-automation/xrp_01_tracker/data",
  "file_pattern": "*.csv"
}
```

### Output Settings
```json
"output": {
  "path": "./data/reports/",
  "file_name_pattern": "trade_execution_report_{date}.csv",
  "date_format": "%Y%m%d"
}
```

### Filtering Options
```json
"filters": {
  "symbols": ["XRPUSD"],
  "sides": ["Buy"],
  "top_rates": {
    "enabled": true,
    "max_pairs": 3
  },
  "additional_symbols": [],
  "date_range": {
    "days_back": 7
  }
}
```

### Column Selection
```json
"columns": {
  "include": ["ID", "DateTime", "Symbol", "Quantity", "Rate", "Side"],
  "rename": {}
}
```

### Scheduling
```json
"scheduling": {
  "enabled": true,
  "hour": 2,
  "minute": 30
}
```

## Usage

### Manual Execution
```bash
python process_trades.py
```

### Setup Automated Scheduling
```bash
./schedule_processor.sh
```

## Output

The processor generates CSV reports containing the filtered trade data with the columns specified in the configuration.
