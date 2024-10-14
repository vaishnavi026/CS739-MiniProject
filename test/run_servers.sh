#!/bin/bash

# Check if the number of servers (n) is provided as an argument
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <number_of_servers>"
    exit 1
fi

# Get the number of servers to run
n=$1
# Starting port number
start_port=50051

# Loop to run n servers
for ((i=0; i<n; i++)); do
    port=$((start_port + i))
    log_file="ACC${port}.log"
    
    # Run the server in the background and redirect output to the log file
    ./server 0.0.0.0:$port $n > "$log_file" 2>&1 &
    
    echo "Started server on port $port, logging to $log_file"
done

# Wait for all background jobs to finish
wait
