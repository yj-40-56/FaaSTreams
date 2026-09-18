#!/bin/bash
set -e

# Purge whatever's still queued in Pub/Sub from previous test runs. Resetting
# Redis alone doesn't touch this — Eventarc's push subscription retries failed
# deliveries with backoff instead of dropping them, so old test runs can leave
# millions of stale messages queued up, silently polluting the next run.
# ais-stream can have more than one subscription at a time (the push ingestor's
# eventarc-managed one, plus a temporary ais-stream-pull if a pull-ingestor test
# is in progress) — purge all of them, not just the first.
SUBSCRIPTIONS=$(gcloud pubsub subscriptions list --filter="topic:ais-stream" --format="value(name)")
if [ -z "$SUBSCRIPTIONS" ]; then
  echo "Could not find any Pub/Sub subscription for topic ais-stream — skipping backlog purge"
else
  NOW="$(date -u +%Y-%m-%dT%H:%M:%S.000Z)"
  while IFS= read -r SUBSCRIPTION; do
    echo "Purging Pub/Sub backlog on $SUBSCRIPTION..."
    gcloud pubsub subscriptions seek "$SUBSCRIPTION" --time="$NOW"
  done <<< "$SUBSCRIPTIONS"
  echo "Backlog purged."
fi

gcloud compute ssh redis-bastion --zone europe-west3-a --command "
  redis-cli -h 10.101.64.19 -p 6379 DEL \
    data:ais_data_v1 \
    analytics-results \
    window:next:ais_data_v1 \
    lock:ais_data_v1:hazard_zones_proximity_alerts
"

echo "Keys deleted. Run the simulation now — waiting for the first data to land in"
echo "data:ais_data_v1 before seeding the window pointer (push ingestor: seconds;"
echo "pull ingestor: up to one Scheduler tick)..."

gcloud compute ssh redis-bastion --zone europe-west3-a --command "
  WAITED=0
  while [ \"\$(redis-cli -h 10.101.64.19 -p 6379 ZCARD data:ais_data_v1)\" = '0' ]; do
    sleep 5
    WAITED=\$((WAITED + 5))
    if [ \$WAITED -ge 300 ]; then
      echo 'No data after 5 minutes — is the simulator running and pointed at the right topic/ingestor?'
      exit 1
    fi
  done
  SCORE=\$(redis-cli -h 10.101.64.19 -p 6379 ZRANGE data:ais_data_v1 0 0 WITHSCORES | tail -1)
  redis-cli -h 10.101.64.19 -p 6379 ZADD window:next:ais_data_v1 \$SCORE hazard_zones_proximity_alerts
  echo \"Seeded window:next:ais_data_v1 with score \$SCORE\"
"
