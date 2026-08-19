#!/bin/bash
# Triggers ingestor-pull to drain whatever the simulator just published, then
# reports PASS/FAIL/SKIP for each pipeline stage: ingestor-pull -> Redis,
# windower dispatch, worker processed, data-sink stored.
#
# Assumes scripts/reset-pipeline.sh --no-wait ran immediately before the simulator,
# so data:ais_data_v1 / analytics-results start at 0 — "stage succeeded" is just
# "count > 0 now", no separate before/after snapshot needed for those two.
#
# Never touches the live windower-tick scheduler job's paused state (see
# terraform/README.md) — ingestor-pull is triggered by a direct HTTP call instead.
set -u

PROJECT=${PROJECT:-faastreams}
REGION=${REGION:-europe-west3}
BASTION_ZONE=${BASTION_ZONE:-europe-west3-a}
REDIS_HOST=${REDIS_HOST:-10.101.64.19}
REDIS_PORT=${REDIS_PORT:-6379}

FULL=false
if [ "${1:-}" = "--full" ]; then
  FULL=true
fi

if [ "$FULL" = true ]; then
  FRESHNESS="110m"
  PULL_TRIGGERS=3
else
  FRESHNESS="10m"
  PULL_TRIGGERS=2
fi

PASS=0
FAIL=0
declare -a RESULTS

redis_cmd() {
  gcloud compute ssh redis-bastion --zone "$BASTION_ZONE" --project "$PROJECT" \
    --command "redis-cli -h $REDIS_HOST -p $REDIS_PORT $*" 2>/dev/null
}

INGESTOR_PULL_URL=$(gcloud functions describe ingestor-pull --gen2 --region "$REGION" \
  --project "$PROJECT" --format="value(serviceConfig.uri)" 2>/dev/null)
if [ -z "$INGESTOR_PULL_URL" ]; then
  echo "Could not resolve ingestor-pull's URL via gcloud — is it deployed?" >&2
  exit 1
fi

echo "Draining ais-stream-pull via $PULL_TRIGGERS direct call(s) to ingestor-pull..."
for i in $(seq 1 "$PULL_TRIGGERS"); do
  echo "  call $i/$PULL_TRIGGERS..."
  curl -s -o /dev/null -w "  -> HTTP %{http_code} (%{time_total}s)\n" --max-time 90 "$INGESTOR_PULL_URL" || true
done

# --- Stage 1: ingested (ingestor-pull -> Redis) ---
INGESTED=$(redis_cmd "ZCARD data:ais_data_v1" | tail -1)
if [ "${INGESTED:-0}" -gt 0 ] 2>/dev/null; then
  RESULTS+=("PASS|1/4 ingestor-pull -> Redis|ZCARD data:ais_data_v1 = $INGESTED")
  PASS=$((PASS + 1))
else
  RESULTS+=("FAIL|1/4 ingestor-pull -> Redis|ZCARD data:ais_data_v1 = ${INGESTED:-0}")
  FAIL=$((FAIL + 1))
fi

# --- Stage 2: windower dispatched ---
# windower's triggerWorker() only logs on failure, so there's no direct "dispatch
# succeeded" log line on the windower side. Use worker's own "Received window" log
# (src/worker/handler.py) as the observable signal that windower's POST arrived.
WINDOWER_LOG=$(gcloud logging read \
  "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"worker\" AND textPayload:\"Received window\"" \
  --project "$PROJECT" --freshness "$FRESHNESS" --limit 1 --format="value(timestamp)" 2>/dev/null)
if [ -n "$WINDOWER_LOG" ]; then
  RESULTS+=("PASS|2/4 windower dispatched|log hit at $WINDOWER_LOG")
  PASS=$((PASS + 1))
else
  RESULTS+=("FAIL|2/4 windower dispatched|no dispatch log found in last $FRESHNESS")
  FAIL=$((FAIL + 1))
fi

# --- Stage 3: worker processed ---
WORKER_LOG=$(gcloud logging read \
  "resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"worker\" AND textPayload=~\"result\\(s\\):\"" \
  --project "$PROJECT" --freshness "$FRESHNESS" --limit 1 --format="value(timestamp)" 2>/dev/null)
if [ -n "$WORKER_LOG" ]; then
  RESULTS+=("PASS|3/4 worker processed|log hit at $WORKER_LOG")
  PASS=$((PASS + 1))
else
  RESULTS+=("FAIL|3/4 worker processed|no [Worker:*] result log found in last $FRESHNESS")
  FAIL=$((FAIL + 1))
fi

# --- Stage 4: sink stored ---
STORED=$(redis_cmd "ZCARD analytics-results" | tail -1)
if [ "${STORED:-0}" -gt 0 ] 2>/dev/null; then
  RESULTS+=("PASS|4/4 data-sink stored|ZCARD analytics-results = $STORED")
  PASS=$((PASS + 1))
else
  RESULTS+=("FAIL|4/4 data-sink stored|ZCARD analytics-results = ${STORED:-0}")
  FAIL=$((FAIL + 1))
fi

echo ""
echo "--- benchmark-check results ---"
for r in "${RESULTS[@]}"; do
  IFS='|' read -r status label detail <<< "$r"
  printf "[%s] %-24s %s\n" "$status" "$label" "$detail"
done
echo ""
echo "$PASS passed, $FAIL failed"

[ "$FAIL" -eq 0 ]
