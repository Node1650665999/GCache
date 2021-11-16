#!/bin/bash
trap "rm server;kill 0" EXIT

/usr/local/go/bin/go build -o server
./server -port=8001 &
./server -port=8002 &
./server -port=8003 -api=1 &

sleep 2
echo -e "\n>>> start test \n\n"
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &
curl "http://localhost:9999/api?key=Tom" &

wait
