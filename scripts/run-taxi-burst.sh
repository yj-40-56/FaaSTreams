#!/bin/bash
set -e

# Short (~2 min), cheap taxi-record burst using real T-Drive events sliced from the
# trainday workload — used by `make benchmark` (SOURCE=tdrive_data_v1, the default).
# Not the full 96-minute run; see scripts/run-tdrive-replay.sh / `make benchmark-full`
# for that.

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DURATION_S=${DURATION_S:-120}

SRC_CSV="$REPO_ROOT/data/tdrive_workload_trainday.csv"
BURST_CSV="$REPO_ROOT/data/tdrive_workload_burst.csv"

if [ ! -f "$SRC_CSV" ]; then
  echo "$SRC_CSV not found — needed to slice a short taxi burst." >&2
  echo "Generate it via scripts/local-data-analysis/tdrive_build_trainday.py + tdrive_build_workload.py (see train_day.py)." >&2
  exit 1
fi

awk -F',' -v d="$DURATION_S" 'NR==1 || $2<=d' "$SRC_CSV" > "$BURST_CSV"
echo "Wrote $(wc -l < "$BURST_CSV") lines (incl. header) covering the first ${DURATION_S}s to $BURST_CSV"

INGESTOR_PULL_URL=$(gcloud functions describe ingestor-pull --gen2 --region europe-west3 \
  --project faastreams --format='value(serviceConfig.uri)')

cd "$REPO_ROOT/src/simulator"
go run ./cmd/tdrive-replay \
  --project faastreams \
  --topic ais-stream \
  --source tdrive_data_v1 \
  --input ../../data/tdrive_workload_burst.csv \
  --results ../../results_tdrive_burst.csv \
  --trigger-url "$INGESTOR_PULL_URL" \
  --run-duration "$DURATION_S" \
  --drain-time 30s
