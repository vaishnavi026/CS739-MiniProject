#!/bin/bash

SERVER_ADDRESS="0.0.0.0:50051"
CLIENT_EXEC="./hotperf"
NUM_CLIENTS=5
NUM_RUNS=1
REQUESTS_PER_CLIENT=100000

total_time_ns=0
total_requests=$((NUM_CLIENTS * REQUESTS_PER_CLIENT))  # 10 requests per client, multiplied by number of clients

echo "Running performance tests..."
echo "Server Address: $SERVER_ADDRESS"
echo "Clients: $NUM_CLIENTS, Requests per Client: $REQUESTS_PER_CLIENT"

export DYLD_LIBRARY_PATH=.

for (( i=1; i<=NUM_RUNS; i++ ))
do
    echo "Starting test run #$i..."
    
    start_time_ns="$(gdate +%s%N)"

    for (( j=1; j<=NUM_CLIENTS; j++ ))
    do
        $CLIENT_EXEC $REQUESTS_PER_CLIENT $SERVER_ADDRESS &
    done

    # Wait for all clients to finish
    wait

    end_time_ns="$(gdate +%s%N)"

    elapsed_time_ns=$((end_time_ns - start_time_ns))
    echo "Test run #$i finished in $elapsed_time_ns nanoseconds."
    total_time_ns=$((total_time_ns + elapsed_time_ns))
done

average_time_ns=$(echo "scale=2; $total_time_ns / $NUM_RUNS" | bc)

throughput=$(echo "scale=6; ($total_requests / $average_time_ns) * 1000000000" | bc)

average_latency_ns=$(echo "scale=2; ($average_time_ns/1000000) / $NUM_CLIENTS * $NUM_RUNS)" | bc)


# Output the results
echo "--------------------------------------------------"
echo "Performance Test Summary:"
echo "Total Runs: $NUM_RUNS"
echo "Average Execution Time: $average_time_ns nanoseconds"
echo "Total Requests Per Run: $total_requests"
echo "Throughput: $throughput requests/second"
echo "Average Latency per Request: $average_latency_ns nanoseconds"
echo "--------------------------------------------------"

# Optionally log results to a file
echo "Logging results to performance_log.txt"
echo "Date: $(date)" >> performance_log.txt
echo "Clients: $NUM_CLIENTS, Requests per Client: $REQUESTS_PER_CLIENT" >> performance_log.txt
echo "Average Execution Time: $average_time_ns nanoseconds" >> performance_log.txt
echo "Throughput: $throughput requests/second" >> performance_log.txt
echo "Average Latency per Request: $average_latency_ns nanoseconds" >> performance_log.txt
echo "--------------------------------------------------" >> performance_log.txt
