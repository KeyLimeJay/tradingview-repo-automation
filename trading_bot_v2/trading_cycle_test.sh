#!/bin/bash

# Generic Trading Cycle Test Script
# Supports testing both buy and sell cycles for any trading pair

# Colors for better readability
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Function to check if a trade was successful
check_trade_success() {
  local result=$1
  local success=$(echo $result | grep -o '"success":[^,}]*' | cut -d':' -f2)
  
  if [[ "$success" == "true" ]]; then
    echo -e "${GREEN}PASS${NC}"
    return 0
  else
    local error=$(echo $result | grep -o '"error":"[^"]*"' | cut -d'"' -f4)
    echo -e "${RED}FAIL${NC}: $error"
    return 1
  fi
}

# Script header
echo -e "${BLUE}=== Trading Cycle Test Script ===${NC}"
echo -e "${CYAN}This script tests complete trading cycles with the trading bot${NC}"

# Prompt for cycle type
echo -e "\n${YELLOW}Select cycle type:${NC}"
echo "1) Buy Cycle  (Start with buy, transition to short, then back to long)"
echo "2) Sell Cycle (Start with sell, transition to long, then back to short)"
read -p "Enter your choice (1 or 2): " cycle_choice

if [[ "$cycle_choice" != "1" && "$cycle_choice" != "2" ]]; then
  echo -e "${RED}Invalid choice. Please enter 1 or 2.${NC}"
  exit 1
fi

if [[ "$cycle_choice" == "1" ]]; then
  cycle_type="Buy Cycle"
else
  cycle_type="Sell Cycle"
fi

# Prompt for trading pair
echo -e "\n${YELLOW}Select trading pair:${NC}"
echo "1) ETH/USDC"
echo "2) BTC/USDC"
echo "3) SOL/USDC"
echo "4) Custom pair"
read -p "Enter your choice (1-4): " pair_choice

case $pair_choice in
  1) symbol="ETH/USDC" ;;
  2) symbol="BTC/USDC" ;;
  3) symbol="SOL/USDC" ;;
  4)
    read -p "Enter base asset (e.g., ETH, BTC): " base_asset
    read -p "Enter quote asset (e.g., USDC): " quote_asset
    base_asset=$(echo $base_asset | tr '[:lower:]' '[:upper:]')  # Convert to uppercase
    quote_asset=$(echo $quote_asset | tr '[:lower:]' '[:upper:]')  # Convert to uppercase
    symbol="${base_asset}/${quote_asset}"
    ;;
  *) 
    echo -e "${RED}Invalid choice. Defaulting to ETH/USDC.${NC}"
    symbol="ETH/USDC"
    ;;
esac

# Prompt for price
read -p "Enter the price to use for testing: " price

# Validate price input
if ! [[ "$price" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
  echo -e "${RED}Invalid price format. Please enter a valid number.${NC}"
  exit 1
fi

# Prompt for timeframe
echo -e "\n${YELLOW}Select timeframe:${NC}"
echo "1) 1m  (1 minute)"
echo "2) 5m  (5 minutes)"
echo "3) 15m (15 minutes)"
echo "4) 1h  (1 hour)"
echo "5) 4h  (4 hours)"
read -p "Enter your choice (1-5): " tf_choice

case $tf_choice in
  1) timeframe="1m" ;;
  2) timeframe="5m" ;;
  3) timeframe="15m" ;;
  4) timeframe="1h" ;;
  5) timeframe="4h" ;;
  *) echo -e "${RED}Invalid choice. Defaulting to 1m.${NC}"; timeframe="1m" ;;
esac

echo -e "\n${BLUE}=== Test Configuration ===${NC}"
echo -e "Cycle Type: ${YELLOW}$cycle_type${NC}"
echo -e "Trading Pair: ${YELLOW}$symbol${NC}"
echo -e "Price: ${YELLOW}$price${NC}"
echo -e "Timeframe: ${YELLOW}$timeframe${NC}"
echo -e "\nStarting test sequence..."

# Check initial position
echo -e "\n${YELLOW}[STEP 1]${NC} Checking initial position..."
initial_position=$(curl -s -X GET http://localhost:6100/positions)
echo $initial_position | jq .

# Execute first event based on cycle type
if [[ "$cycle_choice" == "1" ]]; then
  # Buy Cycle: Event 1 (Buy Signal with no position)
  echo -e "\n${YELLOW}[STEP 2]${NC} Executing Event 1: Buy 1 unit..."
  event1_result=$(curl -s -X POST http://localhost:6100/webhook \
    -H "Content-Type: application/json" \
    -d "{
      \"symbol\": \"$symbol\",
      \"message\": \"Trend Buy!\",
      \"price\": $price,
      \"timeFrame\": \"$timeframe\"
    }")
  echo $event1_result | jq .
  echo -n "Event 1 (Initial Buy) Status: "
  check_trade_success "$event1_result"
  event1_status=$?
  
  # Wait for execution
  echo "Waiting for execution to complete..."
  sleep 3
  
  # Check position after first event
  echo -e "\n${YELLOW}[STEP 3]${NC} Verifying position after first event..."
  position_after_first=$(curl -s -X GET http://localhost:6100/positions)
  echo $position_after_first | jq .
  
  # Execute Event 4: Sell Signal with 1 unit long
  echo -e "\n${YELLOW}[STEP 4]${NC} Executing Event 4: Transition to short..."
  event2_result=$(curl -s -X POST http://localhost:6100/webhook \
    -H "Content-Type: application/json" \
    -d "{
      \"symbol\": \"$symbol\",
      \"message\": \"Trend Sell!\",
      \"price\": $price,
      \"timeFrame\": \"$timeframe\"
    }")
  echo $event2_result | jq .
  echo -n "Event 4 (Transition to Short) Status: "
  check_trade_success "$event2_result"
  event2_status=$?
  
else
  # Sell Cycle: Event 2 (Sell Signal with no position)
  echo -e "\n${YELLOW}[STEP 2]${NC} Executing Event 2: Open repo, Sell 1 unit..."
  event1_result=$(curl -s -X POST http://localhost:6100/webhook \
    -H "Content-Type: application/json" \
    -d "{
      \"symbol\": \"$symbol\",
      \"message\": \"Trend Sell!\",
      \"price\": $price,
      \"timeFrame\": \"$timeframe\"
    }")
  echo $event1_result | jq .
  echo -n "Event 2 (Initial Sell) Status: "
  check_trade_success "$event1_result"
  event1_status=$?
  
  # Wait for execution
  echo "Waiting for execution to complete..."
  sleep 3
  
  # Check position after first event
  echo -e "\n${YELLOW}[STEP 3]${NC} Verifying position after first event..."
  position_after_first=$(curl -s -X GET http://localhost:6100/positions)
  echo $position_after_first | jq .
  
  # Execute Event 3: Buy Signal with short position
  echo -e "\n${YELLOW}[STEP 4]${NC} Executing Event 3: Transition to long..."
  event2_result=$(curl -s -X POST http://localhost:6100/webhook \
    -H "Content-Type: application/json" \
    -d "{
      \"symbol\": \"$symbol\",
      \"message\": \"Trend Buy!\",
      \"price\": $price,
      \"timeFrame\": \"$timeframe\"
    }")
  echo $event2_result | jq .
  echo -n "Event 3 (Transition to Long) Status: "
  check_trade_success "$event2_result"
  event2_status=$?
fi

# Wait for execution
echo "Waiting for execution to complete..."
sleep 3

# Check position after second event
echo -e "\n${YELLOW}[STEP 5]${NC} Verifying position after second event..."
position_after_second=$(curl -s -X GET http://localhost:6100/positions)
echo $position_after_second | jq .

# Execute third event based on cycle type
if [[ "$cycle_choice" == "1" ]]; then
  # Buy Cycle: Event 3 (Buy Signal with short position)
  echo -e "\n${YELLOW}[STEP 6]${NC} Executing Event 3: Transition back to long..."
  event3_result=$(curl -s -X POST http://localhost:6100/webhook \
    -H "Content-Type: application/json" \
    -d "{
      \"symbol\": \"$symbol\",
      \"message\": \"Trend Buy!\",
      \"price\": $price,
      \"timeFrame\": \"$timeframe\"
    }")
  echo $event3_result | jq .
  echo -n "Event 3 (Transition to Long) Status: "
  check_trade_success "$event3_result"
  event3_status=$?
  
else
  # Sell Cycle: Event 4 (Sell Signal with long position)
  echo -e "\n${YELLOW}[STEP 6]${NC} Executing Event 4: Transition back to short..."
  event3_result=$(curl -s -X POST http://localhost:6100/webhook \
    -H "Content-Type: application/json" \
    -d "{
      \"symbol\": \"$symbol\",
      \"message\": \"Trend Sell!\",
      \"price\": $price,
      \"timeFrame\": \"$timeframe\"
    }")
  echo $event3_result | jq .
  echo -n "Event 4 (Transition to Short) Status: "
  check_trade_success "$event3_result"
  event3_status=$?
fi

# Wait for execution
echo "Waiting for execution to complete..."
sleep 3

# Check final position
echo -e "\n${YELLOW}[STEP 7]${NC} Verifying final position..."
final_position=$(curl -s -X GET http://localhost:6100/positions)
echo $final_position | jq .

# Final status report
echo -e "\n${BLUE}=== Test Summary for $cycle_type ===${NC}"
echo -e "Trading Pair: ${YELLOW}$symbol${NC}"
echo -e "Price: ${YELLOW}$price${NC}"
echo -e "Timeframe: ${YELLOW}$timeframe${NC}"

if [[ "$cycle_choice" == "1" ]]; then
  # Buy Cycle Summary
  echo -n "Event 1 (Initial Buy): "
  if [ $event1_status -eq 0 ]; then
    echo -e "${GREEN}PASS${NC}"
  else
    echo -e "${RED}FAIL${NC}"
  fi
  
  echo -n "Event 4 (Transition to Short): "
  if [ $event2_status -eq 0 ]; then
    echo -e "${GREEN}PASS${NC}"
  else
    echo -e "${RED}FAIL${NC}"
  fi
  
  echo -n "Event 3 (Transition to Long): "
  if [ $event3_status -eq 0 ]; then
    echo -e "${GREEN}PASS${NC}"
  else
    echo -e "${RED}FAIL${NC}"
  fi
  
else
  # Sell Cycle Summary
  echo -n "Event 2 (Initial Sell): "
  if [ $event1_status -eq 0 ]; then
    echo -e "${GREEN}PASS${NC}"
  else
    echo -e "${RED}FAIL${NC}"
  fi
  
  echo -n "Event 3 (Transition to Long): "
  if [ $event2_status -eq 0 ]; then
    echo -e "${GREEN}PASS${NC}"
  else
    echo -e "${RED}FAIL${NC}"
  fi
  
  echo -n "Event 4 (Transition to Short): "
  if [ $event3_status -eq 0 ]; then
    echo -e "${GREEN}PASS${NC}"
  else
    echo -e "${RED}FAIL${NC}"
  fi
fi

# Overall test status
if [ $event1_status -eq 0 ] && [ $event2_status -eq 0 ] && [ $event3_status -eq 0 ]; then
  echo -e "\n${GREEN}OVERALL TEST RESULT: PASS${NC} - All trades executed successfully"
else
  echo -e "\n${RED}OVERALL TEST RESULT: FAIL${NC} - One or more trades failed"
fi