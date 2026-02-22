#!/bin/sh
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic reddit.posts.raw --replication-factor 3 --partitions 3
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic reddit.posts.refresh --replication-factor 3 --partitions 3
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic reddit.posts.dlq --replication-factor 3 --partitions 1
echo "All topics created."
kafka-topics --bootstrap-server kafka:9092,kafka2:9092,kafka3:9092 --describe
