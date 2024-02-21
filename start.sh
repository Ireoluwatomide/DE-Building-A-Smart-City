#!/bin/bash

# Start Spark
start-spark.sh
sleep 3

# Start Zookeeper
brew services start zookeeper
sleep 3

# Start Kafka
brew services start kafka
sleep 3

# List Brew services
brew services list
