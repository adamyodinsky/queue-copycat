#!/bin/bash

counter=0

# Loop to produce messages every 0.5 seconds
while true; do
  # Produce a message "hello world" with the current timestamp
  echo "Message $counter from producer at $(date)" | kafka-console-producer --broker-list $BROKER --topic $TOPIC
  # Increment the counter
  ((counter++))
  sleep 1
done
