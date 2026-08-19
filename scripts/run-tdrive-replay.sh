#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 <trigger-url> [trainday|volatile|steady]" >&2
  echo "  trigger-url: windower's ProcessWindows URL under push, or ingestor-pull's IngestPull URL under pull" >&2
  exit 2
fi

TRIGGER_URL=$1
PROFILE=${2:-trainday}
case "$PROFILE" in
  trainday|volatile|steady) ;;
  *) echo "profile must be 'trainday', 'volatile', or 'steady'" >&2; exit 2 ;;
esac

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO_ROOT/src/simulator"

EXTRA_ARGS=()
if [[ "$PROFILE" == "trainday" ]]; then
  EXTRA_ARGS+=(--run-duration 5760 --drain-time 70s)
fi

go run ./cmd/tdrive-replay \
  --project faastreams \
  --topic ais-stream \
  --source tdrive_data_v1 \
  --input "../../data/tdrive_workload_${PROFILE}.csv" \
  --results "../../results_tdrive_${PROFILE}.csv" \
  --trigger-url "$TRIGGER_URL" \
  "${EXTRA_ARGS[@]}"
