#!/bin/bash

set -e

trap "/sandboxes/rsandbox/stop_all && exit" SIGINT SIGTERM

echo "Starting servers..."

/sandboxes/rsandbox/start_all > /dev/null

echo "Both servers started"

while :
do
  # noop while waiting for signals
  sleep 60
done

