#!/bin/bash

while true
do
  echo "Starting agents..."
  ./start.sh

  echo "Running for 1 hour..."
  sleep 3600

  echo "Stopping agents..."
  ./stop.sh

  echo "Cooling down for 5 minutes..."
  sleep 300
done