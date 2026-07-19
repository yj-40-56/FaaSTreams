#!/bin/bash
set -euo pipefail

REDIS_HOST="10.101.64.19"
SUB_SPE="spe-input-sub"

gcloud compute ssh redis-bastion --zone=europe-west3-a --command="
  redis-cli -h $REDIS_HOST FLUSHALL
"

NOW=$(date -u +%Y-%m-%dT%H:%M:%SZ)

gcloud pubsub subscriptions seek "$SUB_SPE" --time="$NOW"
