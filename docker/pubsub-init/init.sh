#!/bin/sh
# Creates the topic and pull subscription inside the Pub/Sub emulator. In
# production Terraform's modules/pubsub does this; the emulator starts empty,
# so the local stack needs an equivalent one-shot step.
set -eu

HOST="${PUBSUB_EMULATOR_HOST:?PUBSUB_EMULATOR_HOST required}"
PROJECT="${PUBSUB_PROJECT_ID:?PUBSUB_PROJECT_ID required}"
TOPIC="${PUBSUB_TOPIC_ID:?PUBSUB_TOPIC_ID required}"
SUB="${PUBSUB_PULL_SUBSCRIPTION_ID:?PUBSUB_PULL_SUBSCRIPTION_ID required}"

echo "[pubsub-init] waiting for emulator at ${HOST}"
until curl -sf "http://${HOST}/v1/projects/${PROJECT}/topics" >/dev/null 2>&1; do
  sleep 1
done

echo "[pubsub-init] creating topic ${TOPIC}"
curl -sf -X PUT "http://${HOST}/v1/projects/${PROJECT}/topics/${TOPIC}" \
  -H 'Content-Type: application/json' -d '{}' >/dev/null

echo "[pubsub-init] creating subscription ${SUB}"
curl -sf -X PUT "http://${HOST}/v1/projects/${PROJECT}/subscriptions/${SUB}" \
  -H 'Content-Type: application/json' \
  -d "{\"topic\":\"projects/${PROJECT}/topics/${TOPIC}\",\"ackDeadlineSeconds\":60}" >/dev/null

echo "[pubsub-init] done"
