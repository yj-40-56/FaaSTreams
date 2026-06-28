# FaaSTreams Terraform

## Prerequisites

- Docker (for running Terraform)
- `gcloud` CLI authenticated (`gcloud auth application-default login`)
- GCS state bucket exists (`make bucket-init` if not)

## First-time setup

```bash
make init
make build-worker ENV=baseline   # build and push the worker Docker image
make apply ENV=baseline
```

## Running a benchmark

Every run requires a Redis flush first. Stale `window_end` keys from a previous session cause the coordinator to replay thousands of empty historical windows before reaching current data.

```bash
# 1. Flush Redis state
make flush-redis ENV=baseline

# 2. Run the simulator (~96 seconds, window fires at ~60s)
cd ../src/simulator
PUBSUB_PROJECT_ID=faastreams PUBSUB_TOPIC_ID=ais-stream-baseline CSV_PATH=../../data/ais.csv go run .

# 3. Read results from Cloud Logging (run while simulator is active or immediately after)
gcloud logging read \
  'resource.type="cloud_run_revision" AND (resource.labels.service_name="coordinator-baseline" OR resource.labels.service_name="worker-baseline") AND textPayload!=""' \
  --project=faastreams --limit=50 --format="value(timestamp,textPayload)" \
  --freshness=5m
```

## Teardown

```bash
make destroy ENV=baseline
```

## Benchmark configurations

Three pre-configured environments are available:

| ENV | Window | Worker RAM | What it tests |
|-----|--------|------------|---------------|
| `baseline` | 60s | 2 GB | control |
| `benchmark-small-window` | 30s | 2 GB | coordinator overhead at higher window frequency |
| `benchmark-high-memory` | 60s | 4 GB | whether more RAM reduces query latency |

You can also override the window size on any environment without editing the tfvars file:

```bash
make apply ENV=baseline WINDOW_SIZE=15
make flush-redis ENV=baseline
# run simulator with PUBSUB_TOPIC_ID=ais-stream-baseline
make destroy ENV=baseline
```

`-var` takes precedence over `-var-file`, so `WINDOW_SIZE` overrides whatever is in the environment's tfvars.

## simulator scaleFactor

The simulator compresses CSV timestamps by `scaleFactor` so data plays back faster than recorded. The value must satisfy:

```
CSV_duration_seconds / scaleFactor > window_size_seconds
```

Adjust `scaleFactor` in `src/simulator/simulator.go` to match your CSV file before running.

## Other targets

| Command | Description |
|---------|-------------|
| `make plan ENV=baseline` | Preview changes without applying |
| `make validate ENV=baseline` | Validate Terraform config |
| `make purge-env ENV=baseline` | Delete orphaned GCP resources not in Terraform state |
| `make build-worker ENV=baseline` | Rebuild and push the worker Docker image |
