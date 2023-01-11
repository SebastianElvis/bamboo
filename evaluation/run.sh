#!/bin/bash

SERVER_PID_FILE=server.pid

if [ -z "${SERVER_PID}" ]; then
    echo "Process id for servers is written to location: {$SERVER_PID_FILE}"
    ./server -id $1 -log_dir=. -log_level=debug &
    echo $! >> ${SERVER_PID_FILE}
else
    echo "Servers are already started in this folder."
    exit 0
fi
