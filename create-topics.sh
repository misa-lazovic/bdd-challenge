#!/bin/bash

echo "Waiting for Kafka to start..."
cub kafka-ready -b kafka:9093 1 20

echo "Creating Kafka topic..."
kafka-topics.sh --create --topic view_log --bootstrap-server kafka:9093 --partitions 3 --replication-factor 1

echo "Listing Kafka topics..."
kafka-topics.sh --list --bootstrap-server kafka:9093
