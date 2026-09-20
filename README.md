# (Advanced) Distributed Systems Prototyping - FaaS Data Stream Processing

## Overview

This repository contains the codebase for our FaaS (Function-as-a-Service) based data stream processing system.

In the final production architecture, live data will be pushed from external sources through a data queue and ingested into Redis, which serves as our temporary data storage. FaaS workers will then process this streaming data.

---

## Provisioning with Terraform

`terraform/` provisions the whole pipeline in one GCP project: the network,
Memorystore Redis and bastion, the `ais-stream` topic and pull subscription, the
four services on direct VPC egress, and the four Cloud Scheduler jobs that
restart ingestor sessions.

```bash
export PROJECT=faas-pj       # the only per-deployment setting
make terraform-init
make terraform-plan          # preview infra changes, safe anytime
make terraform-apply         # apply after reviewing the plan
make help                    # full target list
```

**[`DEPLOYING.md`](DEPLOYING.md) is the walkthrough for a fresh project**, and
assumes no Terraform experience. `terraform/README.md` covers the stack itself:
what it manages, why four Scheduler jobs, and the constraints (instance caps,
shared vCPU, public endpoints). `scripts/deploy-sandbox.sh` remains the
alternative `gcloud` path — use one or the other per project, not both.

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

The stack mirrors the deployed topology in `terraform/main.tf`. Every application
container runs the same source and the same entry point Terraform deploys, with
`FUNCTION_TARGET` set to the module's `build_config.entry_point`. Managed services
are substituted only where they have to be: the Pub/Sub emulator for Pub/Sub,
fake-gcs-server for the query-config bucket, a curl loop for Cloud Scheduler, and
plain Redis for Memorystore.

Cloud Tasks and the pinger are deliberately not modelled. `IngestPull` paces the
windower from inside its own session, so the pinger is a redundant second trigger
path that exists only in the cloud.

Service endpoints, once up:

| Service    | Local URL               |
|------------|-------------------------|
| worker     | `http://localhost:8080` |
| ingestor   | `http://localhost:8081` |
| windower   | `http://localhost:8082` |
| data-sink  | `http://localhost:8083` |

Results land in the `analytics-results` sorted set in Redis:

```bash
docker compose -f docker/docker-compose.dev.yml exec redis redis-cli zrange analytics-results 0 -1
```

Note that the bundled simulator publishes `ais_data_v1` only, so the queries bound
to `t-drive_data_v1` and to `generic` log empty windows on every tick.

To delete the setup run:

```bash
docker compose -f docker/docker-compose.dev.yml down -v
```

## E2E Example - Google Cloud

The pipeline runs as four independently deployed Cloud Functions (gen2): `ingestor`, `windower`, `worker` and `data-sink`. Data flows `simulator → Pub/Sub → ingestor → Redis → windower → worker → data-sink`.

Deploy them with Terraform (`DEPLOYING.md`), or with `scripts/deploy-sandbox.sh` for the `gcloud` path. The invocations below document how the hand-built `faastreams` project was deployed; they name that project's network and are kept for reference.

```bash
# ingestor-pull - HTTP-triggered by Cloud Scheduler, drains a pull subscription
gcloud functions deploy ingestor-pull --gen2 --runtime go126 --region europe-west3 \
  --memory 2048Mi --cpu 2 --source src/ingestor --entry-point IngestPull \
  --trigger-http --allow-unauthenticated --network default \
  --subnet projects/faastreams/regions/europe-west3/subnetworks/default \
  --env-vars-file env/gcloud-env-ingestor-pull.yaml --max-instances 4 --concurrency 1

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

Then run the simulator to publish mock AIS data to the topic the pull subscription reads:

```bash
cd scripts && bash run-simulator.sh
```

It is configured by env vars in that script, not by the query config. `SIM_SCALE_FACTOR` is CSV duration over desired real duration, and `SIM_RUNTIME` caps the run in real time. The ingestor must already be pulling when it starts: a paused Scheduler job means nothing drains the subscription, and Pub/Sub delivers the resulting backlog out of order.

## Pull-Based Ingestion

`ingestor-pull` (`src/ingestor/pull.go`, entry point `IngestPull`) is the only ingestor deployed. Rather than one invocation per Pub/Sub message, it is HTTP-triggered by Cloud Scheduler on a fixed interval. Each invocation keeps a Receive session open for up to `maxSessionDuration`, writes what it drains into `data:<source>`, and nudges windower's `ProcessWindows` directly (fire-and-forget) every `interval`.

Terminology: a **Tick** is one Scheduler-triggered invocation; a **Drain** is the repeated pulling inside it, bounded by the session deadline and ended early once the subscription goes quiet.

Sessions overlap by design. A Scheduler job will not start a run while its own previous attempt is open, so one job holds a session roughly `maxSessionDuration / 120s` of the time; several jobs on staggered schedules are what keep the subscription continuously pulled. Overlap is safe because the writes are idempotent `ZADD`s and the windower takes the minimum watermark across live instances.

The subscription is created separately, since a topic fans out to every subscription independently:

```bash
bash scripts/create-pull-subscription.sh   # creates ais-stream-pull fresh
bash scripts/deploy-sandbox.sh --only ingestor-pull
# then point a Cloud Scheduler job at ingestor-pull's URL
```

`scripts/delete-pull-subscription.sh` removes it again so it does not accumulate backlog between runs.

The push ingestor (`IngestEvent` in `src/ingestor/main.go`) is retired but still compiles; it fell behind under load, where per-message invocation overhead dominated. The two were never deployed at the same time. Its entry point is left in place because `init()` runs for every entry point built from this source directory, so removing it is a separate change.

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

| Component             | Key                                                             | Mode         | Source                                                                             |
| --------------------- | --------------------------------------------------------------- | ------------ | ---------------------------------------------------------------------------------- |
| ingestor              | `data:<source>`                                                 | writes       | `src/ingestor/main.go` (`dataKey = "data"`)                                        |
| ingestor              | `watermark:<source>`                                            | writes       | `src/ingestor/watermark.go` (hash, one field per instance)                         |
| ingestor              | `watermark:floor:<source>`                                      | **reads**    | `src/ingestor/watermark.go` (`seedWatermarks`); owned by the windower               |
| ingestor (push, retired) | `active:<source>`, `session:<source>:<id>`                   | writes       | `src/ingestor/main.go`                                                             |
| windower              | `data:<source>`, `window:next:<source>`, `lock:<source>:<query>` | reads/writes | `src/windower/main.go`                                                             |
| windower              | `pending:<source>`, `pending:meta:<source>`                     | reads/writes | `src/windower/main.go` (worker-confirmed retention)                                |
| windower              | `watermark:<source>`                                            | reads        | `src/windower/main.go` (`closeTime`, minimum across live instances)                |
| windower              | `watermark:floor:<source>`, `watermark:stall:<source>`          | writes       | `src/windower/main.go` (monotonic floor, stall hatch)                              |
| worker                | `data:<source>`                                                 | reads        | `src/worker/fetch.py` (`DATA_KEY_PREFIX` + `data_source` from the trigger payload) |
| worker                | `pending:<source>`, `inflight:<source>:<query>::<start>:<end>`   | reads/writes | `src/worker/fetch.py` (lease, cleared on confirmation)                             |
| data-sink             | `analytics-results` (fixed, not per-source)                     | writes       | `src/data-sink/handler.py` (`REDIS_KEY` env var)                                   |

Note: key prefixes are independent literals in each component -- nothing enforces that they agree beyond convention. A prior refactor changed the ingestor's key scheme without updating the worker's, silently breaking the pipeline (worker read from a dead key and always returned zero results) until the mismatch was found and fixed. Keep this in mind when changing key naming on any one side.

Two cross-component couplings, neither enforced:

- `watermark:<source>` is written by the ingestor and read by the windower, which takes the **minimum across live instances** and drops a field once it goes stale.
- `watermark:floor:<source>` is owned by the windower but **read by the ingestor** at session start, so a cold-started instance adopts the promise already acted on rather than computing one from the partial view its first messages give.
