#!/usr/bin/env bash
set -euo pipefail

BROKER="${BROKER:-127.0.0.1:9092}"
CONTAINER="${CONTAINER:-feedsystem-zero-kafka}"
PARTITIONS="${PARTITIONS:-6}"
REPLICATION="${REPLICATION:-1}"

topics=(
  "interaction.like.events"
  "interaction.comment.events"
  "video.stat.delta.events"
  "feed.video.events"
  "notification.events"
  "social.follow.events"
)

for topic in "${topics[@]}"; do
  docker exec "${CONTAINER}" kafka-topics \
    --bootstrap-server "${BROKER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}"
done

docker exec "${CONTAINER}" kafka-topics --bootstrap-server "${BROKER}" --list
