#!/bin/bash

# Stop Spark
stop-spark.sh

# Stop Kafka and Zookeeper
brew services stop kafka
brew services stop zookeeper

# List Brew services
brew services list
