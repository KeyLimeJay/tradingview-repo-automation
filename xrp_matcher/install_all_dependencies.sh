#!/bin/bash
# Installation script for XRP Matcher dependencies

echo "Installing required Python packages..."

# Function to check if a Python package is installed
function is_installed() {
    python3 -c "import $1" >/dev/null 2>&1
    return $?
}

# Install python-dotenv
if ! is_installed dotenv; then
    echo "Installing python-dotenv..."
    pip install python-dotenv
else
    echo "python-dotenv is already installed."
fi

# Install websocket-client
if ! is_installed websocket; then
    echo "Installing websocket-client..."
    pip install websocket-client
else
    echo "websocket-client is already installed."
fi

# Install requests
if ! is_installed requests; then
    echo "Installing requests..."
    pip install requests
else
    echo "requests is already installed."
fi

# Install websockets
if ! is_installed websockets; then
    echo "Installing websockets..."
    pip install websockets
else
    echo "websockets is already installed."
fi

# Optional: Install other potentially useful packages
pip install asyncio aiohttp

echo "All dependencies installed successfully!"
echo "You can now run the XRP Matcher application."
