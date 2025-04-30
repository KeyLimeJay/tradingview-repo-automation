#!/bin/bash
echo "Testing Sell Start Sequence (2→3→4→3→4) on TM5 (5m timeframe)"
echo "====================================="

# Define variables
HOST="localhost"
PORT="6100"
SYMBOL="ETH/USDC"
PRICE="1750"
TIMEFRAME="5m"  # TM5 uses 5m timeframe

echo "1. Event 2: Sell Signal (Open Repo, Sell 1 unit)"
curl -X POST http://$HOST:$PORT/webhook \
  -H "Content-Type: application/json" \
  -d "{
    \"timeFrame\": \"$TIMEFRAME\",
    \"symbol\": \"$SYMBOL\",
    \"message\": \"Trend Sell!\",
    \"price\": $PRICE
  }"
echo -e "\n\nWaiting 5 seconds...\n"
sleep 5

echo "2. Event 3: Buy Signal (Buy 2 units, Close Repo)"
curl -X POST http://$HOST:$PORT/webhook \
  -H "Content-Type: application/json" \
  -d "{
    \"timeFrame\": \"$TIMEFRAME\",
    \"symbol\": \"$SYMBOL\",
    \"message\": \"Trend Buy!\",
    \"price\": $PRICE
  }"
echo -e "\n\nWaiting 5 seconds...\n"
sleep 5

echo "3. Event 4: Sell Signal (Sell 1 unit, Open Repo, Sell 1 unit)"
curl -X POST http://$HOST:$PORT/webhook \
  -H "Content-Type: application/json" \
  -d "{
    \"timeFrame\": \"$TIMEFRAME\",
    \"symbol\": \"$SYMBOL\",
    \"message\": \"Trend Sell!\",
    \"price\": $PRICE
  }"
echo -e "\n\nWaiting 5 seconds...\n"
sleep 5

echo "4. Event 3: Buy Signal (Buy 2 units, Close Repo)"
curl -X POST http://$HOST:$PORT/webhook \
  -H "Content-Type: application/json" \
  -d "{
    \"timeFrame\": \"$TIMEFRAME\",
    \"symbol\": \"$SYMBOL\",
    \"message\": \"Trend Buy!\",
    \"price\": $PRICE
  }"
echo -e "\n\nWaiting 5 seconds...\n"
sleep 5

echo "5. Event 4: Sell Signal (Sell 1 unit, Open Repo, Sell 1 unit)"
curl -X POST http://$HOST:$PORT/webhook \
  -H "Content-Type: application/json" \
  -d "{
    \"timeFrame\": \"$TIMEFRAME\",
    \"symbol\": \"$SYMBOL\",
    \"message\": \"Trend Sell!\",
    \"price\": $PRICE
  }"
echo -e "\n\nWaiting 5 seconds...\n"
sleep 5

echo "6. Final Buy and Close Repo to return to zero"
curl -X POST http://$HOST:$PORT/webhook \
  -H "Content-Type: application/json" \
  -d "{
    \"timeFrame\": \"$TIMEFRAME\",
    \"symbol\": \"$SYMBOL\",
    \"message\": \"Trend Buy!\",
    \"price\": $PRICE
  }"
echo -e "\n\nSequence test completed.\n"

# Check sequence health
echo "Checking sequence health:"
curl -X GET http://$HOST:$PORT/sequence-health
echo -e "\n"

# Reset sequence for clean state
echo "Resetting sequence:"
curl -X POST http://$HOST:$PORT/sequence-reset \
  -H "Content-Type: application/json" \
  -d "{}"
echo -e "\n"
