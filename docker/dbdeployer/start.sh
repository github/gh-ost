#!/bin/bash

set -e

trap "/sandboxes/rsandbox/stop_all && exit" SIGINT SIGTERM

/sandboxes/rsandbox/start_all

while :
do
  # noop while waiting for signals
  sleep 60
done

