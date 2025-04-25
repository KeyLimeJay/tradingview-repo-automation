#!/bin/bash

# Configuration
API_URL="http://localhost:6101"  # Modify if your server runs on a different host/port
SYMBOLS=("BTC/USDT" "ETH/USDT")  # Test with both BTC and ETH
ACCOUNT="default"                # Default account, modify if needed

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to send webhook signal
send_signal() {
    local symbol=$1
    local message=$2
    local price=$3
    local timeframe=$4

    echo -e "${YELLOW}Sending signal: $message for $symbol at price $price${NC}"
    
    curl -s -X POST "${API_URL}/webhook" \
        -H "Content-Type: application/json" \
        -d "{
            \"symbol\": \"$symbol\",
            \"message\": \"$message\",
            \"price\": $price,
            \"timeFrame\": \"$timeframe\"
        }"
    
    echo -e "\n"
    # Small delay to let the server process the request
    sleep 2
}

# Function to check positions
check_positions() {
    echo -e "${YELLOW}Checking current positions...${NC}"
    curl -s -X GET "${API_URL}/positions" | jq '.'
    echo -e "\n"
}

# Function to reset event counters (optional, if your API supports it)
# If not available, you'll need to restart the server between test sequences
reset_counters() {
    echo -e "${YELLOW}Resetting event counters...${NC}"
    # This endpoint doesn't exist in your code, you might need to add it
    # curl -s -X POST "${API_URL}/reset"
    echo "WARNING: Your API doesn't have a reset endpoint. You may need to restart the server."
    echo -e "\n"
}

# Function to verify balances are zero
verify_zero_balances() {
    local response=$(curl -s -X GET "${API_URL}/positions")
    
    for symbol in "${SYMBOLS[@]}"; do
        local balance=$(echo $response | jq -r ".accounts.\"${ACCOUNT}\".positions.\"${symbol}\".balance")
        if [ "$balance" != "0" ]; then
            echo -e "${RED}FAILED: ${symbol} balance is not zero: ${balance}${NC}"
            return 1
        fi
    done
    
    echo -e "${GREEN}SUCCESS: All balances are zero${NC}"
    return 0
}

# Run Sequence 1: Event 1 -> Event 4 -> Event 3 -> Event 4 -> Close Repo
run_sequence_1() {
    echo -e "${GREEN}=== Running Test Sequence 1 ===${NC}"
    
    for symbol in "${SYMBOLS[@]}"; do
        echo -e "${GREEN}Testing ${symbol}${NC}"
        
        # Event 1: Buy Signal
        send_signal "${symbol}" "Trend Buy!" "50000" "1h"
        
        # Event 4: Sell Signal (1st sequence - Sell)
        send_signal "${symbol}" "Trend Sell!" "51000" "1h"
        
        # Event 4: Sell Signal (2nd sequence - Open Repo)
        send_signal "${symbol}" "Trend Sell!" "51500" "1h"
        
        # Event 4: Sell Signal (3rd sequence - Sell)
        send_signal "${symbol}" "Trend Sell!" "52000" "1h"
        
        # Event 3: Buy Signal (1st sequence - Buy)
        send_signal "${symbol}" "Trend Buy!" "49000" "1h"
        
        # Event 3: Buy Signal (2nd sequence - Close Repo)
        send_signal "${symbol}" "Trend Buy!" "48500" "1h"
        
        # Event 3: Buy Signal (3rd sequence - Buy)
        send_signal "${symbol}" "Trend Buy!" "48000" "1h"
        
        # Event 4: Sell Signal (1st sequence - Sell)
        send_signal "${symbol}" "Trend Sell!" "49500" "1h"
        
        # Check final positions
        check_positions
    done
    
    # Verify all balances are zero
    verify_zero_balances
}

# Run Sequence 2: Event 2 -> Event 3 -> Event 4 -> Event 3 -> Sell
run_sequence_2() {
    echo -e "${GREEN}=== Running Test Sequence 2 ===${NC}"
    
    for symbol in "${SYMBOLS[@]}"; do
        echo -e "${GREEN}Testing ${symbol}${NC}"
        
        # Event 2: Sell Signal (1st sequence - Open Repo)
        send_signal "${symbol}" "Trend Sell!" "50000" "1h"
        
        # Event 2: Sell Signal (2nd sequence - Sell)
        send_signal "${symbol}" "Trend Sell!" "50500" "1h"
        
        # Event 3: Buy Signal (1st sequence - Buy)
        send_signal "${symbol}" "Trend Buy!" "49000" "1h"
        
        # Event 3: Buy Signal (2nd sequence - Close Repo)
        send_signal "${symbol}" "Trend Buy!" "48500" "1h"
        
        # Event 3: Buy Signal (3rd sequence - Buy)
        send_signal "${symbol}" "Trend Buy!" "48000" "1h"
        
        # Event 4: Sell Signal (1st sequence - Sell)
        send_signal "${symbol}" "Trend Sell!" "49500" "1h"
        
        # Check final positions
        check_positions
    done
    
    # Verify all balances are zero
    verify_zero_balances
}

# Main execution

# Check if server is up
echo -e "${YELLOW}Checking if server is up...${NC}"
health_check=$(curl -s -X GET "${API_URL}/health")
if [ $? -ne 0 ]; then
    echo -e "${RED}ERROR: Server is not running at ${API_URL}${NC}"
    exit 1
fi
echo -e "${GREEN}Server is up and running!${NC}\n"

# Initial position check
check_positions

# Run Sequence 1
run_sequence_1

# You might need to restart the server here to reset event counters
echo -e "${YELLOW}You may need to restart the server to reset event counters before running sequence 2${NC}"
read -p "Press enter to continue to sequence 2 (restart server first if needed)..."

# Reset counters (if supported)
reset_counters

# Run Sequence 2
run_sequence_2

echo -e "${GREEN}All tests completed!${NC}"