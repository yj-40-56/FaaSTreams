# (Advanced) Distributed Systems Prototyping - FaaS Data Stream Processing

## Overview

This repository contains the codebase for our FaaS (Function-as-a-Service) based data stream processing system.

Live data is pulled from a Pub/Sub subscription by `ingestor-pull` on a schedule and written into Redis, which serves as our temporary data storage. FaaS workers then process this streaming data. **Push and pull ingestors must never run simultaneously** — they write the same Redis keys (see "Push vs. pull ingestion" below).

## Provisioning and benchmarking

`terraform/` provisions **all** of the infrastructure and nothing else: the five
services (`ingestor-pull`, `windower`, `worker`, `data-sink`, `pinger`), the shared
Memorystore Redis instance, the Serverless VPC Access connector they reach it
through, the `ais-stream` Pub/Sub topic + subscription, the `faastreams-queue` Cloud
Tasks queue, and the two Cloud Scheduler jobs. Benchmarks are *not* run through
Terraform — that is this Makefile's job. From the repo root:

```bash
make terraform-plan          # preview infra changes, safe anytime
make terraform-apply         # apply after reviewing the plan
make benchmark                # short (~2 min) end-to-end run, pass/fail per stage
make benchmark-full           # full 96-minute train-day replay
make save-results             # save the last benchmark window to results/
```

See `terraform/README.md` for first-time setup (importing the already-live
resources into state) and `make help` for the full target list. Every individual
script under `scripts/` remains directly runnable for targeted testing — the
Makefile only composes them.

---

## Worker

FaaS worker (`src/worker`) that, given a time window and query from the windower, fetches the matching AIS records from Redis, loads them into DuckDB, runs the configured query, and emits proximity warnings for vessels approaching defined hazard zones.

### Setup

```bash
cd src/worker
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Running

The worker is an HTTP-triggered function (see `docker/worker/Dockerfile`); make sure Redis is reachable first (e.g. via `docker/docker-compose.dev.yml`, see below).

```bash
functions-framework --target=handler --source=handler.py --port=8080
```

In production this endpoint is called by the windower, which POSTs a JSON body shaped like:

```json
{
  "window_start": 1750000000,
  "window_end": 1750000060,
  "query_name": "hazard_zones_proximity_alerts",
  "data_source": "ais_data_v1",
  "columns": { "...": "..." },
  "reference_tables": { "...": "..." },
  "query": "SELECT ...",
  "return_type": "spatial",
  "is_alert": true,
  "alert_format": "{mmsi} ({name}) — ..."
}
```

`window_start`/`window_end` are Unix timestamps bounding the score range read from `data:<data_source>` in Redis (see Redis Key Layout below).

## E2E Example - local

For a local demonstration run terminal command:

```bash
docker compose -f docker/docker-compose.dev.yml up --build
```

When using this setup, ensure that the data folder contains a .csv with its header (column names).

To delete the setup run:

```bash
docker compose -f docker/docker-compose.dev.yml down
```

## E2E Example - Google Cloud

The pipeline runs as five independently deployed Cloud Functions (gen2):
`ingestor-pull`, `windower`, `worker`, `data-sink`, and `pinger`
(fans out windower triggers via Cloud Tasks; deployed from `src/scheduler_task_queue`
— renamed by hand on 2026-08-21, see `terraform/README.md`). Data flows
`simulator → Pub/Sub (ais-stream) → ingestor-pull → Redis → windower → worker → data-sink`.

The easiest way to provision all of this is `make terraform-apply` from the repo
root (see "Provisioning and benchmarking" above) — it manages the live, unsuffixed
versions of every resource below. To deploy a single component by hand instead
(e.g. while iterating on one function), the individual scripts still work:
`scripts/deploy-ingestor-pull.sh`, `scripts/deploy-windower.sh`,
`scripts/deploy-worker.sh`, `scripts/deploy-data-sink.sh`,
`scripts/deploy-pinger.sh`.

```bash
# ingestor-pull - HTTP-triggered by Cloud Scheduler, drains ais-stream-pull on a tick
gcloud functions deploy ingestor-pull --gen2 --runtime go126 --region europe-west3 \
  --memory 1024Mi --cpu 2 --source src/ingestor --entry-point IngestPull \
  --trigger-http --allow-unauthenticated --network default \
  --subnet projects/faastreams/regions/europe-west3/subnetworks/default \
  --env-vars-file env/gcloud-env-ingestor-pull.yaml --max-instances 2 \
  --concurrency 1 --timeout 120

# windower - HTTP-triggered, invoked to process pending windows
gcloud functions deploy windower --gen2 --runtime go126 --region europe-west3 \
  --memory 256Mi --source src/windower --entry-point ProcessWindows \
  --trigger-http --allow-unauthenticated --network default \
  --subnet projects/faastreams/regions/europe-west3/subnetworks/default \
  --env-vars-file env/gcloud-env-windower.yaml

# worker - HTTP-triggered by the windower, runs DuckDB queries over a Redis window
gcloud functions deploy worker --gen2 --runtime python312 --region europe-west3 \
  --memory 2048Mi --max-instances 4 --source src/worker --entry-point handler \
  --trigger-http --allow-unauthenticated --network default \
  --subnet projects/faastreams/regions/europe-west3/subnetworks/default \
  --env-vars-file env/gcloud-env-worker.yaml --timeout 540

# data-sink - HTTP-triggered by the worker, persists query results
gcloud functions deploy data-sink --gen2 --runtime python312 --region europe-west3 \
  --memory 256Mi --source src/data-sink --entry-point handler \
  --trigger-http --allow-unauthenticated --network default \
  --subnet projects/faastreams/regions/europe-west3/subnetworks/default \
  --env-vars-file env/gcloud-env-data-sink.yaml
```

Then run the simulator (`scripts/run-simulator.sh`, or from `src/simulator`
directly) to publish mock AIS data to the Pub/Sub topic `ingestor-pull` consumes
via the `ais-stream-pull` subscription:

```bash
PUBSUB_PROJECT_ID=faastreams PUBSUB_TOPIC_ID=ais-stream CONFIG_BUCKET=faastreams-config \
  CONFIG_OBJECT=query-config.yaml SOURCE_NAME=ais_data_v1 go run .
```

`ingestor-pull` only pulls when invoked — either by its Cloud Scheduler job or by
calling its HTTP endpoint directly (what `make benchmark` does).

## Push vs. pull ingestion

`src/ingestor/main.go` (entry point `IngestEvent`) is the older push ingestor,
triggered per-message by a Pub/Sub push subscription. `src/ingestor/pull.go` (entry
point `IngestPull`, deployed as `ingestor-pull`) is the current live ingestor: HTTP
triggered by Cloud Scheduler on a fixed interval, each invocation (a **Tick**)
drains the `ais-stream-pull` pull subscription until idle. See `CONTEXT.md` for the
Push/Pull Ingestor, Tick, and Drain terminology.

**They must never both be deployed at once** — both write to the same
`data:<source>` Redis keys, and a topic fans out to every subscription
independently, so running both means double-processing. Terraform only manages
`ingestor-pull`; the push `ingestor` has no Terraform resource and should only be
deployed manually, with `ingestor-pull` torn down first, for A/B comparison
purposes.

## Query Config

The query/source definitions (`query-config.yaml`) used by the simulator and worker are stored in GCS at `gs://faastreams-config/query-config.yaml`.

To view its contents:

```bash
gsutil cat gs://faastreams-config/query-config.yaml
```

To download it locally:

```bash
gsutil cp gs://faastreams-config/query-config.yaml ./query-config.yaml
```

## Redis Key Layout

| Component             | Key                                                                          | Source                                                                              |
| --------------------- | ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| ingestor writes       | `data:<source>`                                                              | `src/ingestor/main.go` (push) and `pull.go` (pull; live) — both use `dataKey = "data"` |
| windower reads/writes | `data:<source>`, `window:next:<source>`, `lock:<source>:<query>`             | `src/windower/main.go`                                                              |
| worker reads          | `data:<source>`                                                              | `src/worker/fetch.py` (`DATA_KEY_PREFIX` + `data_source` from the trigger payload)  |
| data-sink writes      | `analytics-results` (fixed, not per-source — single combined results stream) | `src/data-sink/handler.py` (`REDIS_KEY` env var, defaults to `"analytics-results"`) |

Note: `"data"` is defined as an independent literal in each of the three components above — nothing enforces that they agree beyond convention. A prior refactor changed the ingestor's key scheme without updating the worker's, silently breaking the pipeline (worker read from a dead key and always returned zero results) until the mismatch was found and fixed. Keep this in mind when changing key naming on any one side.
