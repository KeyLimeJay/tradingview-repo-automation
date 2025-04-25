#!/bin/bash

# Configuration
API_URL="http://localhost:6101"  # Using port from your config
SYMBOLS=("BTC/USDC" "ETH/USDC")  # Updated to match your trading pairs in config
ACCOUNT="default"                # Using your default account

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
    sleep 3
}

# Function to check positions
check_positions() {
    echo -e "${YELLOW}Checking current positions...${NC}"
    curl -s -X GET "${API_URL}/positions" | jq '.'
    echo -e "\n"
}

# Function to reset event counters (called before each sequence)
reset_counters() {
    echo -e "${YELLOW}Resetting event counters...${NC}"
    echo -e "${RED}NOTE: Event counters need to be reset.${NC}"
    echo -e "${RED}You may need to restart the server or manually reset.${NC}"
    sleep 1
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
        
        local repo=$(echo $response | jq -r ".accounts.\"${ACCOUNT}\".positions.\"${symbol}\".repo")
        if [ "$repo" != "0" ]; then
            echo -e "${RED}FAILED: ${symbol} repo is not zero: ${repo}${NC}"
            return 1
        fi
    done
    
    echo -e "${GREEN}SUCCESS: All balances and repos are zero${NC}"
    return 0
}

# Run Sequence 1: Event 1 -> Event 4 -> Event 3 -> Event 4 -> Close Repo
run_sequence_1() {
    echo -e "${GREEN}=== Running Test Sequence 1 ===${NC}"
    
    for symbol in "${SYMBOLS[@]}"; do
        echo -e "${GREEN}Testing ${symbol}${NC}"
        
        # Event 1: Buy Signal
        send_signal "${symbol}" "Trend Buy!" "50000" "1h"
        check_positions
        
        # Event 4: Sell Signal (1st sequence - Sell)
        send_signal "${symbol}" "Trend Sell!" "51000" "1h"
        check_positions
        
        # Event 3: Buy Signal (1st sequence - Buy)
        send_signal "${symbol}" "Trend Buy!" "49000" "1h"
        check_positions
        
        # Event 3: Buy Signal (2nd sequence - Close Repo)
        send_signal "${symbol}" "Trend Buy!" "48500" "1h"
        check_positions
        
        # Event 3: Buy Signal (3rd sequence - Buy)
        send_signal "${symbol}" "Trend Buy!" "48000" "1h"
        check_positions
        
        # Event 4: Sell Signal (1st sequence - Sell)
        send_signal "${symbol}" "Trend Sell!" "49500" "1h"
        check_positions
        
        # Final check
        echo -e "${GREEN}Sequence 1 completed for ${symbol}${NC}"
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
        check_positions
        
        # Event 2: Sell Signal (2nd sequence - Sell)
        send_signal "${symbol}" "Trend Sell!" "50500" "1h"
        check_positions
        
        # Event 3: Buy Signal (1st sequence - Buy)
        send_signal "${symbol}" "Trend Buy!" "49000" "1h"
        check_positions
        
        # Event 3: Buy Signal (2nd sequence - Close Repo)
        send_signal "${symbol}" "Trend Buy!" "48500" "1h"
        check_positions
        
        # Event 3: Buy Signal (3rd sequence - Buy)
        send_signal "${symbol}" "Trend Buy!" "48000" "1h"
        check_positions
        
        # Event 4: Sell Signal (1st sequence - Sell)
        send_signal "${symbol}" "Trend Sell!" "49500" "1h"
        check_positions
        
        # Final check
        echo -e "${GREEN}Sequence 2 completed for ${symbol}${NC}"
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

# Prompt for reset before sequence 2
read -p "Press enter to continue to sequence 2 (restart server first if needed)..."

# Reset counters (if supported)
reset_counters

# Run Sequence 2
run_sequence_2

echo -e "${GREEN}All tests completed!${NC}"