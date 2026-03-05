#!/bin/sh
set -e

BS="kafka:9092,kafka2:9092,kafka3:9092"
echo "Creating Kafka topics for all platforms..."

# Reddit
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic reddit.posts.raw     --replication-factor 3 --partitions 3
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic reddit.posts.refresh --replication-factor 3 --partitions 3
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic reddit.posts.dlq     --replication-factor 3 --partitions 1

# Hacker News
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic hackernews.stories.raw --replication-factor 3 --partitions 3
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic hackernews.stories.dlq --replication-factor 3 --partitions 1

# Bluesky (6 partitions — firehose volume)
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic bluesky.posts.raw --replication-factor 3 --partitions 6
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic bluesky.posts.dlq --replication-factor 3 --partitions 1

# YouTube
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic youtube.comments.raw --replication-factor 3 --partitions 3
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic youtube.comments.dlq --replication-factor 3 --partitions 1

# Unified signals (all platforms feed into these)
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic signals.normalised --replication-factor 3 --partitions 6
kafka-topics --bootstrap-server $BS --create --if-not-exists --topic signals.dlq        --replication-factor 3 --partitions 1

echo "Waiting for all topics on all 3 brokers..."
for topic in \
  reddit.posts.raw reddit.posts.refresh reddit.posts.dlq \
  hackernews.stories.raw hackernews.stories.dlq \
  bluesky.posts.raw bluesky.posts.dlq \
  youtube.comments.raw youtube.comments.dlq \
  signals.normalised signals.dlq; do
  until kafka-topics --bootstrap-server $BS --describe --topic "$topic" 2>/dev/null | grep -q "ReplicationFactor: 3"; do
    echo "  waiting for $topic..."
    sleep 2
  done
  echo "  $topic ready."
done

echo "All topics confirmed."
