#!/bin/bash

# Create the main project directory - using a variable for the directory name
PROJECT_DIR="trading_bot_v2"

echo "Creating directory structure in $PROJECT_DIR..."

# Create the main project directory
mkdir -p $PROJECT_DIR

# Create the config directory and credentials subdirectory
mkdir -p $PROJECT_DIR/config/credentials

# Create the source code directory
mkdir -p $PROJECT_DIR/src

# Create the logs directory
mkdir -p $PROJECT_DIR/logs

# Create empty files for the main files
touch $PROJECT_DIR/main.py
touch $PROJECT_DIR/requirements.txt
touch $PROJECT_DIR/README.md

# Create empty files for the source code
touch $PROJECT_DIR/src/account_manager.py
touch $PROJECT_DIR/src/auto_position_manager.py
touch $PROJECT_DIR/src/config_manager.py
touch $PROJECT_DIR/src/position_websocket.py
touch $PROJECT_DIR/src/trading_bot.py
touch $PROJECT_DIR/src/trading_utils.py

# Create empty config files
touch $PROJECT_DIR/config/config.json
touch $PROJECT_DIR/config/credentials/default.json
touch $PROJECT_DIR/config/credentials/tm5.json
touch $PROJECT_DIR/config/credentials/tm6.json

echo "Directory structure created successfully in $PROJECT_DIR!"
ls -la $PROJECT_DIR