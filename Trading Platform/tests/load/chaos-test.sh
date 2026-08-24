#!/bin/bash

# Chaos Testing Script for Trading Engine
echo "Starting Trading Engine Chaos Test..."

# Start the trading engine in background using Maven
mvn exec:java -Dexec.mainClass="Main" > /dev/null 2>&1 &
ENGINE_PID=$!

echo "Engine started with PID $ENGINE_PID"
sleep 5 # Wait for startup

echo "Running normal load..."
# Simulate some k6 load
# k6 run tests/load/ws-order-throughput.js &
# K6_PID=$!

# Let it run
sleep 5

echo "Injecting Chaos: Random network latency (simulated by pausing)"
kill -STOP $ENGINE_PID
sleep 2
kill -CONT $ENGINE_PID

echo "Injecting Chaos: Overloading CPU"
for i in {1..4}; do yes > /dev/null & done
YES_PIDS=$(jobs -p)
sleep 5
kill $YES_PIDS

echo "Chaos Test Complete. Shutting down engine."
kill -9 $ENGINE_PID

echo "Verifying journal integrity..."
# Trigger the deterministic replay test to ensure journal is intact
mvn test -Dtest=ReplayVerifierTest

if [ $? -eq 0 ]; then
    echo "SUCCESS: Journal is intact after chaos."
else
    echo "FAILURE: State mismatch or corruption detected."
    exit 1
fi
