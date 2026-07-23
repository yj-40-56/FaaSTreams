# (Advanced) Distributed Systems Prototyping - FaaS Data Stream Processing

## Overview

This repository contains the codebase for our FaaS (Function-as-a-Service) based data stream processing system.

In the final production architecture, live data will be pushed from external sources through a data queue and ingested into Redis, which serves as our temporary data storage. FaaS workers will then process this streaming data.

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

The pipeline runs as four independently deployed Cloud Functions (gen2): `ingestor`, `windower`, `worker` and `data-sink`. Data flows `simulator → Pub/Sub → ingestor → Redis → windower → worker → data-sink`.

See `scripts/deploy-ingestor.sh`, `scripts/deploy-windower.sh`, `scripts/deploy-worker.sh`, and `scripts/deploy-data-sink.sh` for the exact `gcloud functions deploy` invocations.

```bash
# ingestor - triggered by messages on the Pub/Sub topic
gcloud functions deploy ingestor --gen2 --runtime go126 --region europe-west3 \
  --memory 2048Mi --cpu 2 --source src/ingestor --entry-point IngestEvent \
  --trigger-topic ais-stream --network default \
  --subnet projects/faastreams/regions/europe-west3/subnetworks/default \
  --env-vars-file env/gcloud-env-ingestor.yaml --max-instances 6 --concurrency 20

# windower - HTTP-triggered, invoked to process pending windows
gcloud functions deploy windower --gen2 --runtime go126 --region europe-west3 \
  --memory 256Mi --source src/windower --entry-point ProcessWindows \
  --trigger-http --allow-unauthenticated --network default \
  --subnet projects/faastreams/regions/europe-west3/subnetworks/default \
  --env-vars-file env/gcloud-env-windower.yaml

# worker - HTTP-triggered by the windower, runs DuckDB queries over a Redis window
gcloud functions deploy worker --gen2 --runtime python312 --region europe-west3 \
  --memory 1024Mi --source src/worker --entry-point handler \
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

Then run the simulator (from `src/simulator`) to publish mock AIS data to the Pub/Sub topic the ingestor is subscribed to:

```bash
PUBSUB_PROJECT_ID=faastreams PUBSUB_TOPIC_ID=ais-stream CONFIG_BUCKET=faastreams-config \
  CONFIG_OBJECT=query-config.yaml SOURCE_NAME=ais_data_v1 go run .
```

## Experimental: Pull-Based Ingestion

`ingestor-pull` (`src/ingestor/pull.go`, entry point `IngestPull`) is an alternate `ingestor` implementation being evaluated as a fix for push-ingestion backlog under load (see [[ingestor-throughput-capacity]] in project memory). Instead of one invocation per Pub/Sub message, it's HTTP-triggered by Cloud Scheduler on a fixed interval; each invocation (a **Tick**) drains a pull subscription until idle, writes everything to the same `data:<source>` Redis keys the push ingestor uses, then calls windower's `ProcessWindows` directly (fire-and-forget) instead of relying on windower's own schedule. See `CONTEXT.md` for the Push/Pull Ingestor, Tick, and Drain terminology.

This is a parallel experiment, not a replacement: the push and pull ingestors are never deployed at the same time, and are compared sequentially (A/B), since both write into the same Redis keys and a topic fans out to every subscription independently.

To run a pull-ingestor test:

```bash
bash scripts/create-pull-subscription.sh   # creates ais-stream-pull fresh
gcloud functions delete ingestor --region europe-west3 --quiet   # push ingestor must not be live
bash scripts/deploy-ingestor-pull.sh
# point Cloud Scheduler's job at ingestor-pull's URL instead of windower's for the duration of the test
```

Afterwards, redeploy the push ingestor (`scripts/deploy-ingestor.sh`), point Scheduler back at windower, and run `scripts/delete-pull-subscription.sh` so `ais-stream-pull` doesn't linger and accumulate backlog between runs.

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
| ingestor writes       | `data:<source>`                                                              | `src/ingestor/main.go` (`dataKey = "data"`)                                         |
| windower reads/writes | `data:<source>`, `window:next:<source>`, `lock:<source>:<query>`             | `src/windower/main.go`                                                              |
| worker reads          | `data:<source>`                                                              | `src/worker/fetch.py` (`DATA_KEY_PREFIX` + `data_source` from the trigger payload)  |
| data-sink writes      | `analytics-results` (fixed, not per-source — single combined results stream) | `src/data-sink/handler.py` (`REDIS_KEY` env var, defaults to `"analytics-results"`) |

Note: `"data"` is defined as an independent literal in each of the three components above — nothing enforces that they agree beyond convention. A prior refactor changed the ingestor's key scheme without updating the worker's, silently breaking the pipeline (worker read from a dead key and always returned zero results) until the mismatch was found and fixed. Keep this in mind when changing key naming on any one side.
