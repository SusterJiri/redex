#!/bin/bash

PORT=${1:-6379}

echo "Testing BLPOP with Redis server on port $PORT"
echo "Starting test scenario..."

# Start first BLPOP in background
echo "[client-2] Starting BLPOP pineapple 0"
redis-cli -p $PORT BLPOP pineapple 0 &
PID1=$!

# Small delay then start second BLPOP
echo "[client-3] Starting BLPOP pineapple 0" 
redis-cli -p $PORT BLPOP pineapple 0 &
PID2=$!

# Wait 2 seconds then push an item
# sleep 2
echo "[client-1] Starting RPUSH pineapple apple"
redis-cli -p $PORT RPUSH pineapple apple

# Wait for background processes to complete
echo "Waiting for BLPOP commands to complete..."
wait $PID1
echo "[client-2] BLPOP completed"
wait $PID2  
echo "[client-3] BLPOP completed"

echo "All commands completed!"