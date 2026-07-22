#!/bin/bash
set -euo pipefail

RUNS=3
LOG_DIR="./benchmark-runs"
REDIS_HOST="10.101.64.19"
ZONE="europe-west3-a"
mkdir -p "$LOG_DIR"

echo "Updating config once before the runs"
chmod +x update-config.sh
./update-config.sh

# CSV header
if [ ! -f "$LOG_DIR/run-timestamps.csv" ]; then
  echo "run,run_start,run_end,flink_first_result,faas_first_result" > "$LOG_DIR/run-timestamps.csv"
fi

for i in $(seq 1 $RUNS); do
  RUN_START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "############################################"
  echo "### RUN $i/$RUNS — start: $RUN_START"
  echo "############################################"

  RUN_LOG="$LOG_DIR/run-${i}-$(date -u +%Y%m%d-%H%M%S).log"

  # Full clean deploy + simulator cycle
  bash reset-benchmark.sh 2>&1 | tee "$RUN_LOG"

  RUN_END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "Run $i finished: $RUN_START -> $RUN_END" | tee -a "$RUN_LOG"

  # Fetch earliest (lowest score = earliest timestamp) result from each pipeline's Redis sorted set
  FLINK_FIRST=$(gcloud compute ssh redis-bastion --zone=$ZONE --command="redis-cli -h $REDIS_HOST ZRANGE flink-results 0 0" 2>/dev/null | tr -d '\r')
  FAAS_FIRST=$(gcloud compute ssh redis-bastion --zone=$ZONE --command="redis-cli -h $REDIS_HOST ZRANGE analytics-results 0 0" 2>/dev/null | tr -d '\r')

  # Escape quotes
  FLINK_FIRST_ESCAPED=$(echo "$FLINK_FIRST" | sed 's/"/""/g')
  FAAS_FIRST_ESCAPED=$(echo "$FAAS_FIRST" | sed 's/"/""/g')

  echo "$i,$RUN_START,$RUN_END,\"$FLINK_FIRST_ESCAPED\",\"$FAAS_FIRST_ESCAPED\"" >> "$LOG_DIR/run-timestamps.csv"

  if [ "$i" -lt "$RUNS" ]; then
    echo "Waiting 60s before next run."
    sleep 60
  fi
done

echo "All $RUNS runs completed."