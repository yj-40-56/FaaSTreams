#!/bin/bash
set -euo pipefail

RUNS=3
LOG_DIR="./benchmark-runs"
REDIS_HOST="10.101.64.19"
ZONE="europe-west3-a"
BASTION="redis-bastion"
CSV_FILE="$LOG_DIR/faas-stage-latencies.csv"
MAX_WAIT_SECONDS=120

mkdir -p "$LOG_DIR"

# install if needed
# winget install jqlang.jq
# echo "Updating configuration"
# chmod +x update-config.sh
# ./update-config.sh

# CSV
if [ ! -f "$CSV_FILE" ]; then
  echo "run,run_start,run_end,ingestor_start,ingestor_end,windower_start,windower_end,worker_start,worker_end,ingestor_ms,windower_ms,worker_ms,e2e_ms,raw_payload" > "$CSV_FILE"
fi

for i in $(seq 1 $RUNS); do
  RUN_START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "========================================================"
  echo "### RUN $i/$RUNS — start: $RUN_START"
  echo "========================================================"

  RUN_LOG="$LOG_DIR/run-${i}-$(date -u +%Y%m%d-%H%M%S).log"

  # 1. Reset 
  echo "Executing FaaS benchmark run"
  {
    echo "Flush redis"
    gcloud compute ssh "$BASTION" --zone="$ZONE" --tunnel-through-iap \
      --command="redis-cli -h $REDIS_HOST DEL analytics-results data:ais" >/dev/null 2>&1 || true

    echo "Reset Pub/sub subs"
    SEEK_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    gcloud pubsub subscriptions seek ais-stream-pull --time="$SEEK_TIME" --quiet || true
    gcloud pubsub subscriptions seek spe-sub --time="$SEEK_TIME" --quiet || true

    echo "Running simulator"
    bash run-simulator.sh
  } 2>&1 | tee "$RUN_LOG"

  RUN_END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "Run $i finished execution: $RUN_START -> $RUN_END" | tee -a "$RUN_LOG"

  # 2. Fetch from redis
  echo "Waiting for results in Redis"
  FAAS_RAW=""
  ELAPSED=0

  while [ "$ELAPSED" -lt "$MAX_WAIT_SECONDS" ]; do
    FAAS_RAW=$(gcloud compute ssh "$BASTION" --zone="$ZONE" --tunnel-through-iap \
      --command="redis-cli -h $REDIS_HOST ZRANGE analytics-results 0 0" 2>/dev/null | tr -d '\r' || true)

    if [ -n "$FAAS_RAW" ] && [ "$FAAS_RAW" != "(nil)" ]; then
      break
    fi

    sleep 3
    ELAPSED=$(( ELAPSED + 3 ))
  done

  if [ -z "$FAAS_RAW" ] || [ "$FAAS_RAW" = "(nil)" ]; then
    echo "WARNING: No results found in analytics-results after ${MAX_WAIT_SECONDS}s for Run $i!" | tee -a "$RUN_LOG"
    continue
  fi

  # 3. parse timestamps
  T0=$(echo "$FAAS_RAW" | jq -r '.ingestor_start // .t0 // 0')
  T1=$(echo "$FAAS_RAW" | jq -r '.ingestor_end // .t1 // 0')
  T2=$(echo "$FAAS_RAW" | jq -r '.windower_start // .t2 // 0')
  T3=$(echo "$FAAS_RAW" | jq -r '.windower_end // .t3 // 0')
  T4=$(echo "$FAAS_RAW" | jq -r '.worker_start // .t4 // 0')
  T5=$(echo "$FAAS_RAW" | jq -r '.worker_end // .t5 // 0')

  T0=${T0:-0}; T1=${T1:-0}; T2=${T2:-0}; T3=${T3:-0}; T4=${T4:-0}; T5=${T5:-0}

  # durations
  INGESTOR_DUR=$(( T1 - T0 ))
  WINDOWER_DUR=$(( T3 - T2 ))
  WORKER_DUR=$(( T5 - T4 ))
  E2E_DUR=$(( T5 - T0 ))

  echo " -> Ingestor Latency: ${INGESTOR_DUR}ms | Windower: ${WINDOWER_DUR}ms | Worker: ${WORKER_DUR}ms | E2E: ${E2E_DUR}ms"

  # 4. Save parsed results to CSV (escaping JSON quotes)
  FAAS_ESCAPED=$(echo "$FAAS_RAW" | sed 's/"/""/g')
  echo "$i,$RUN_START,$RUN_END,$T0,$T1,$T2,$T3,$T4,$T5,$INGESTOR_DUR,$WINDOWER_DUR,$WORKER_DUR,$E2E_DUR,\"$FAAS_ESCAPED\"" >> "$CSV_FILE"

  # 5. Flush Redis for the next clean run
  echo "Flushing Redis"
  gcloud compute ssh "$BASTION" --zone="$ZONE" --tunnel-through-iap \
    --command="redis-cli -h $REDIS_HOST DEL analytics-results" >/dev/null 2>&1 || true

  if [ "$i" -lt "$RUNS" ]; then
    echo "Waiting 60s before next run."
    sleep 60
  fi
done

echo "All $RUNS runs completed."