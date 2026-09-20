#!/bin/bash
# Measures how window size changes the cost and latency of a window.
#
# Runs one pipeline run per window size, identical apart from the size, and
# writes a manifest the analysis script reads. Every run: swap the query config,
# force a cold start so it is read, purge the Pub/Sub backlog, clear pipeline
# state in Redis, publish for a fixed time, wait for the tail, then capture a
# Redis diagnostic BEFORE the next run wipes it.
#
# Usage:
#   scripts/window-cost-series.sh                      # default series
#   scripts/window-cost-series.sh 60 30 15 12 10       # explicit sizes
#   PROJECT=my-proj WINDOWS_TARGET=12 scripts/window-cost-series.sh
#
# Then:
#   scripts/window-cost-analyse.py <manifest path printed at the end>
#
# Env:
#   PROJECT          GCP project (default faastreams-e2e-0919)
#   REGION           default europe-west3
#   SOURCE           query-config source name (default ais_data_v1)
#   SCALE            SIM_SCALE_FACTOR (default 24)
#   WINDOWS_TARGET   clean windows wanted per size (default 10); publish time is
#                    derived from it, so large windows run proportionally longer.
#                    This is the fix for the 60s arm resting on n=2.
#   OUT_DIR          where the manifest and diagnostics go (default ./window-cost-<ts>)
set -euo pipefail

PROJECT="${PROJECT:-faastreams-e2e-0919}"
REGION="${REGION:-europe-west3}"
SOURCE="${SOURCE:-ais_data_v1}"
SCALE="${SCALE:-24}"
WINDOWS_TARGET="${WINDOWS_TARGET:-10}"
SUBSCRIPTION="${SUBSCRIPTION:-ais-stream-pull}"
TOPIC="${TOPIC:-ais-stream}"
BASE_CONFIG="${BASE_CONFIG:-env/query-config-smoke.yaml}"

SIZES=("$@")
[ ${#SIZES[@]} -eq 0 ] && SIZES=(60 30 15 12 10)

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$REPO/window-cost-$(date -u +%Y%m%d-%H%M%S)}"
MANIFEST="$OUT_DIR/manifest.json"
mkdir -p "$OUT_DIR"

say()  { printf '\n\033[1;34m==>\033[0m %s  \033[2m(%s)\033[0m\n' "$*" "$(date -u +%H:%M:%SZ)"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[fail]\033[0m %s\n' "$*" >&2; exit 1; }

command -v gcloud >/dev/null || die "gcloud not on PATH"
[ -f "$REPO/$BASE_CONFIG" ] || die "missing $BASE_CONFIG"
GO_BIN="${GO_BIN:-$(command -v go || true)}"
if [ -z "$GO_BIN" ]; then
  for c in /usr/local/go/bin/go /usr/lib/golang/bin/go "$HOME/go/bin/go" /snap/bin/go; do
    [ -x "$c" ] && GO_BIN="$c" && break
  done
fi
[ -n "$GO_BIN" ] || die "go not found -- set GO_BIN=/path/to/go"

say "Discovering the pipeline in $PROJECT"
CONFIG_BUCKET="$(gcloud storage ls --project="$PROJECT" 2>/dev/null | grep -- "-config/" | head -1 | sed 's|gs://||;s|/$||')"
[ -n "$CONFIG_BUCKET" ] || die "no config bucket found in $PROJECT"
REDIS_HOST="$(gcloud redis instances list --region="$REGION" --project="$PROJECT" --format='value(host)' | head -1)"
[ -n "$REDIS_HOST" ] || die "no Redis instance in $PROJECT/$REGION"
BASTION="$(gcloud compute instances list --project="$PROJECT" --filter='name~bastion' --format='value(name)' | head -1)"
[ -n "$BASTION" ] || die "no bastion VM found -- needed to reach Redis on its private IP"
BASTION_ZONE="$(gcloud compute instances list --project="$PROJECT" --filter="name=$BASTION" --format='value(zone)' | head -1)"
mapfile -t JOBS < <(gcloud scheduler jobs list --location="$REGION" --project="$PROJECT" --format='value(name.basename())')
[ ${#JOBS[@]} -gt 0 ] || die "no Cloud Scheduler jobs -- nothing drives the ingestor"
printf '  config bucket  %s\n  redis          %s\n  bastion        %s (%s)\n  scheduler jobs %s\n' \
  "$CONFIG_BUCKET" "$REDIS_HOST" "$BASTION" "$BASTION_ZONE" "${#JOBS[@]}"

redis_cmd() { gcloud compute ssh "$BASTION" --zone="$BASTION_ZONE" --project="$PROJECT" \
                --tunnel-through-iap --command "redis-cli -h $REDIS_HOST -p 6379 $*" 2>/dev/null; }

pause_ticks()  { for j in "${JOBS[@]}"; do gcloud scheduler jobs pause  "$j" --location="$REGION" --project="$PROJECT" >/dev/null 2>&1 || true; done; }
resume_ticks() { for j in "${JOBS[@]}"; do gcloud scheduler jobs resume "$j" --location="$REGION" --project="$PROJECT" >/dev/null 2>&1 || true; done; }

cleanup() { warn "interrupted -- leaving ticks paused in $PROJECT; resume with: make scheduler-resume PROJECT=$PROJECT"; }
trap cleanup INT TERM

echo '{"project":"'"$PROJECT"'","region":"'"$REGION"'","source":"'"$SOURCE"'","scale":'"$SCALE"',"runs":[' > "$MANIFEST"
FIRST=1

for SIZE in "${SIZES[@]}"; do
  # Publish long enough that every size yields a comparable number of clean
  # windows. Four extra cover the ramp, the drain-damaged head and the tail,
  # which the analysis discards.
  RUNTIME=$(( SIZE * (WINDOWS_TARGET + 4) ))
  [ "$RUNTIME" -lt 180 ] && RUNTIME=180

  say "WINDOW ${SIZE}s -- publishing ${RUNTIME}s for ~${WINDOWS_TARGET} clean windows"

  CFG="$OUT_DIR/query-config-window-${SIZE}s.yaml"
  python3 - "$REPO/$BASE_CONFIG" "$CFG" "$SIZE" <<'PYEOF'
import sys, yaml
src, dst, size = sys.argv[1], sys.argv[2], int(sys.argv[3])
cfg = yaml.safe_load(open(src))
q = dict(cfg["queries"][0])
q.update(window_type="tumbling", window_size=size)
q.pop("slide", None)
cfg["queries"] = [q]
open(dst, "w").write(
    f"# Generated by window-cost-series.sh: {size}s tumbling, one query.\n"
    f"# A second query would contend for worker instances and confound the\n"
    f"# per-window durations this run exists to measure.\n"
    + yaml.safe_dump(cfg, sort_keys=False, width=100))
PYEOF

  pause_ticks
  gcloud storage cp "$CFG" "gs://$CONFIG_BUCKET/query-config.yaml" --project="$PROJECT" >/dev/null
  # The config is read in init(), so only a new revision picks it up.
  TS=$(date +%s)
  for s in windower ingestor-pull; do
    gcloud run services update "$s" --region="$REGION" --project="$PROJECT" \
      --update-env-vars=CONFIG_RELOAD="$TS" --quiet >/dev/null 2>&1 || die "could not restart $s"
  done

  say "waiting out any open ingestor session"
  sleep 105

  # seek, never delete-and-recreate: deleting the subscription destroys its
  # Terraform-managed pubsub.subscriber binding and the ingestor then silently
  # reads nothing for the whole run.
  gcloud pubsub subscriptions seek "$SUBSCRIPTION" --time="$(date -u +%Y-%m-%dT%H:%M:%SZ)" --project="$PROJECT" >/dev/null
  redis_cmd DEL "window:next:$SOURCE" "data:$SOURCE" "pending:$SOURCE" "pending:meta:$SOURCE" \
                "inflight:$SOURCE" "watermark:$SOURCE" "watermark:floor:$SOURCE" \
                "watermark:stall:$SOURCE" "active:$SOURCE" analytics-results >/dev/null

  resume_ticks

  say "waiting for a healthy pull cycle"
  SINCE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  OK=0
  for _ in $(seq 1 40); do
    if gcloud logging read "resource.labels.service_name=\"ingestor-pull\" AND timestamp>=\"$SINCE\" AND textPayload:\"processed\"" \
         --project="$PROJECT" --limit=1 --format='value(timestamp)' 2>/dev/null | grep -q .; then OK=1; break; fi
    sleep 15
  done
  [ "$OK" = 1 ] || die "no ingestor session within 10 minutes"
  if gcloud logging read "resource.labels.service_name=\"ingestor-pull\" AND timestamp>=\"$SINCE\" AND textPayload:\"PermissionDenied\"" \
       --project="$PROJECT" --limit=1 --format='value(timestamp)' 2>/dev/null | grep -q .; then
    die "ingestor cannot read $SUBSCRIPTION -- the pubsub.subscriber binding is missing"
  fi

  RUN_FROM="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  say "publishing"
  PUBLISHED=$( cd "$REPO/src/simulator" && \
    PUBSUB_PROJECT_ID="$PROJECT" PUBSUB_TOPIC_ID="$TOPIC" SOURCE_NAME="$SOURCE" \
    SIM_CSV_PATH=../../data/ais.csv SIM_TIMESTAMP_FIELD="# Timestamp" \
    SIM_TIMESTAMP_FORMAT="02/01/2006 15:04:05" SIM_RUNTIME="${RUNTIME}s" \
    SIM_SCALE_FACTOR="$SCALE" SIM_SEQ_FIELD=_seq "$GO_BIN" run . 2>&1 \
    | tee "$OUT_DIR/sim-${SIZE}s.log" | grep -oE "total published: [0-9]+" | grep -oE "[0-9]+" | tail -1 )
  PUBLISHED="${PUBLISHED:-0}"
  printf '  published %s events\n' "$PUBLISHED"

  # The tail window closes on the 180s stall hatch, plus slack for the worker.
  say "waiting for the tail window"
  sleep 240
  RUN_TO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  # Captured BEFORE the next run's wipe: if events remain here, they were stored
  # but reached no window, which is the only way to tell that apart from a
  # counting error after the fact.
  say "capturing the Redis leftover diagnostic"
  LEFTOVER="$(redis_cmd ZCARD "data:$SOURCE" | tr -dc '0-9')"
  redis_cmd ZRANGE "data:$SOURCE" 0 0 WITHSCORES > "$OUT_DIR/leftover-${SIZE}s.txt" 2>&1 || true
  redis_cmd ZRANGE "data:$SOURCE" -1 -1 WITHSCORES >> "$OUT_DIR/leftover-${SIZE}s.txt" 2>&1 || true
  printf '  %s event(s) still in data:%s after the run\n' "${LEFTOVER:-?}" "$SOURCE"
  [ "${LEFTOVER:-0}" -gt 0 ] 2>/dev/null && warn "those reached no window -- see leftover-${SIZE}s.txt"

  [ $FIRST -eq 0 ] && echo ',' >> "$MANIFEST"
  FIRST=0
  printf '{"window":%s,"runtime":%s,"from":"%s","to":"%s","published":%s,"leftover":%s}' \
    "$SIZE" "$RUNTIME" "$RUN_FROM" "$RUN_TO" "$PUBLISHED" "${LEFTOVER:-0}" >> "$MANIFEST"
done

echo ']}' >> "$MANIFEST"
trap - INT TERM

say "Series complete"
cat <<EOF
  manifest   $MANIFEST
  analyse    $REPO/scripts/window-cost-analyse.py "$MANIFEST"

  Ticks are still ENABLED in $PROJECT. Pause them when you are done:
    make scheduler-pause PROJECT=$PROJECT
EOF
