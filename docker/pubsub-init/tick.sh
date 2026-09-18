#!/bin/sh
# Stands in for Cloud Scheduler. In production modules/scheduler POSTs to the
# ingestor's IngestPull entry point on a fixed schedule; IngestPull then holds a
# pull session open and paces the windower itself. Same contract, local loop.
set -eu

TARGET="${TICK_URL:?TICK_URL required}"
INTERVAL="${TICK_INTERVAL:-60}"

echo "[tick] waiting for ${TARGET}"
until curl -sf -o /dev/null -X POST "${TARGET}"; do sleep 2; done
echo "[tick] first tick accepted; interval=${INTERVAL}s"

while true; do
  sleep "${INTERVAL}"
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "${TARGET}" || echo 000)
  echo "[tick] POST ${TARGET} -> ${code}"
done
