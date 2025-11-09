#!/bin/bash

set -e  # Exit on any error

# Run the program
./run.sh -p 3

# Parse the number of processes (excluding process 0 which is parent)
NUM_PROCESSES=$(grep -oP 'Process \K\d+(?= \(pid)' events.log | sort -u | wc -l)

echo "Validating events.log for $NUM_PROCESSES processes..."

# Check for each process (including process 0)
for ((i=0; i<=$NUM_PROCESSES; i++)); do
    echo "Checking process $i..."

    # Check if process received all STARTED messages
    if ! grep -q "^Process $i received all STARTED messages$" events.log; then
        echo "ERROR: Process $i did not receive all STARTED messages"
        exit 1
    fi

    # Check if process received all DONE messages
    if ! grep -q "^Process $i received all DONE messages$" events.log; then
        echo "ERROR: Process $i did not receive all DONE messages"
        exit 1
    fi

    # Check if process has DONE its work (except process 0, which is parent)
    if [ $i -ne 0 ]; then
        if ! grep -q "^Process $i has DONE its work$" events.log; then
            echo "ERROR: Process $i has not DONE its work"
            exit 1
        fi
    fi

    echo "  ✓ Process $i validated"
done

echo "All assertions passed!"