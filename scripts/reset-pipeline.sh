#!/bin/bash
set -e

# --source <name>: which data source's Redis keys to reset (default: ais_data_v1).
#   Use tdrive_data_v1 for the T-Drive taxi benchmark path.
# --no-wait: purge Pub/Sub backlog + delete Redis keys only, then return immediately
# — skip waiting for data and seeding window:next. Used by `make benchmark`, which
# triggers ingestor-pull directly right after this runs; windower auto-bootstraps
# window:next to "now" the first time it sees a query with no existing pointer (see
# src/windower/main.go's createWindows), so manual seeding isn't required for a short
# run — it only avoids windower's first window starting slightly after data that
# arrived during the wait, which doesn't matter for a benchmark's pass/fail check.
SOURCE="ais_data_v1"
NO_WAIT=false
while [ $# -gt 0 ]; do
  case "$1" in
    --no-wait) NO_WAIT=true; shift ;;
    --source) SOURCE="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

# Purge whatever's still queued in Pub/Sub from previous test runs. Resetting
# Redis alone doesn't touch this — Eventarc's push subscription retries failed
# deliveries with backoff instead of dropping them, so old test runs can leave
# millions of stale messages queued up, silently polluting the next run.
# ais-stream can have more than one subscription at a time (the push ingestor's
# eventarc-managed one, plus a temporary ais-stream-pull if a pull-ingestor test
# is in progress) — purge all of them, not just the first. Both ais_data_v1 and
# tdrive_data_v1 traffic flow through this same topic (see run-tdrive-replay.sh's
# --topic ais-stream), so this purge step doesn't vary by --source.
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
  redis-cli -h 10.101.64.19 -p 6379 DEL data:${SOURCE} window:next:${SOURCE} analytics-results
  for k in \$(redis-cli -h 10.101.64.19 -p 6379 --scan --pattern 'lock:${SOURCE}:*'); do
    redis-cli -h 10.101.64.19 -p 6379 DEL \"\$k\"
  done
"

echo "Keys deleted for source=${SOURCE}."

if [ "$NO_WAIT" = true ]; then
  echo "--no-wait set, skipping wait-for-data-and-seed step."
  exit 0
fi

if [ "$SOURCE" != "ais_data_v1" ]; then
  echo "Manual window:next seeding below is only wired for ais_data_v1's known query" \
       "name (hazard_zones_proximity_alerts) — skipping it for source=${SOURCE}." \
       "windower will auto-bootstrap window:next:${SOURCE} to 'now' on first tick" \
       "(see src/windower/main.go's createWindows), so this is not required."
  exit 0
fi

echo "Run the simulation now — waiting for the first data to land in"
echo "data:${SOURCE} before seeding the window pointer (push ingestor: seconds;"
echo "pull ingestor: up to one Scheduler tick)..."

gcloud compute ssh redis-bastion --zone europe-west3-a --command "
  WAITED=0
  while [ \"\$(redis-cli -h 10.101.64.19 -p 6379 ZCARD data:${SOURCE})\" = '0' ]; do
    sleep 5
    WAITED=\$((WAITED + 5))
    if [ \$WAITED -ge 300 ]; then
      echo 'No data after 5 minutes — is the simulator running and pointed at the right topic/ingestor?'
      exit 1
    fi
  done
  SCORE=\$(redis-cli -h 10.101.64.19 -p 6379 ZRANGE data:${SOURCE} 0 0 WITHSCORES | tail -1)
  redis-cli -h 10.101.64.19 -p 6379 ZADD window:next:${SOURCE} \$SCORE hazard_zones_proximity_alerts
  echo \"Seeded window:next:${SOURCE} with score \$SCORE\"
"
