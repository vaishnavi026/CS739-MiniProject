#!/bin/bash

SERVER_ADDRESS="0.0.0.0:50051"
CLIENT_EXEC="./read_write_util"
NUM_CLIENTS=5
NUM_RUNS=1
REQUESTS_PER_CLIENT=1
WRITE_KEY=CS739
WRITE_VALUE=DistributedSystemsMIKESWIFT
READ_KEY=CS739

echo "Running multi client correctness test"
echo "Server Address: $SERVER_ADDRESS"
echo "Clients: $NUM_CLIENTS, Requests per Client: $REQUESTS_PER_CLIENT"

echo "Starting test ..."
echo "Writing at the key : $WRITE_KEY with value : $WRITE_VALUE for client 1"

export DYLD_LIBRARY_PATH=.

$CLIENT_EXEC $SERVER_ADDRESS 1 1 $WRITE_KEY $WRITE_VALUE &

echo "Reading the written value from parallel clients"

wait

for (( j=1; j<=NUM_CLIENTS; j++ ))
do
        $CLIENT_EXEC $SERVER_ADDRESS $j 0 $READ_KEY $WRITE_VALUE &
done
wait

echo "Reads completed"

