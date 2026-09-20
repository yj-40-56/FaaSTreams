# (Advanced) Distributed Systems Prototyping - FaaS Data Stream Processing

## Overview

This repository contains the codebase for our FaaS (Function-as-a-Service) based data stream processing system (**FaaSTreams**).

In the final production architecture, live data (e.g. maritime AIS records) will be pushed from external sources through a data queue and ingested into Redis, which serves as our temporary data storage. FaaS workers will then process this streaming data.

---

## Provisioning with Terraform

`terraform/` provisions the whole pipeline in one GCP project: the network,
Memorystore Redis and bastion, the `ais-stream` topic and pull subscription, the
four services on direct VPC egress, and the four Cloud Scheduler jobs that
restart ingestor sessions.

```bash
export PROJECT=your-project-id # required; no default project
make terraform-bucket-init   # once per project
make terraform-init
make terraform-plan          # preview infra changes, safe anytime
make terraform-apply         # apply after reviewing the plan
make help                    # full target list
```

**[`DEPLOYING.md`](DEPLOYING.md) is the walkthrough for a fresh project**, and
assumes no Terraform experience. [terraform/README.md](terraform/README.md) covers the stack itself:
what it manages, why four Scheduler jobs, and the constraints (instance caps,
shared vCPU, public endpoints).

## Worker

FaaS worker (`src/worker`) that, given a time window and query from the windower, fetches the matching spatiotemporal records (e.g. maritime AIS records) from Redis, loads them into DuckDB, runs the configured query, and emits query results or alerts (e.g., proximity warnings for vessels approaching defined hazard zones).

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

From the repository root, place the example AIS dataset at `data/ais.csv`.
Its CSV header (column names) must match the configured schema in
`env/query-config-reference.yaml`. The dataset is not included in the repository.
For another dataset, update the simulator settings in
`docker/docker-compose.dev.yml` and the source/query definitions together.

Start the local demonstration:

```bash
docker compose -f docker/docker-compose.dev.yml up --build
```

The stack mirrors the deployed topology in `terraform/main.tf`. Every application
container runs the same source and the same entry point Terraform deploys, with
`FUNCTION_TARGET` set to the module's `build_config.entry_point`. Managed services
are substituted only where they have to be: the Pub/Sub emulator for Pub/Sub,
fake-gcs-server for the query-config bucket, a curl loop for Cloud Scheduler, and
plain Redis for Memorystore.

Cloud Tasks and the legacy pinger are absent from both the local stack and the
Terraform deployment. `IngestPull` triggers the windower from inside each session.

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

To stop and remove the local containers:

```bash
docker compose -f docker/docker-compose.dev.yml down
```

Add `-v` to also remove Docker volumes associated with the stack. The local Redis
configuration disables persistence, so its results are lost when its container
is removed even without `-v`. The dataset in `data/` is a host bind mount and is
not deleted by either command.

## E2E Example - Google Cloud

The pipeline runs as four independently deployed Cloud Functions (gen2): `ingestor-pull`, `windower`, `worker` and `data-sink`. Data flows `simulator → Pub/Sub → ingestor → Redis → windower → worker → data-sink`.

Deploy with Terraform using [DEPLOYING.md](DEPLOYING.md). From the repository
root, with `PROJECT` still set to the deployed project, enable the four Scheduler
jobs (new deployments start paused):

```bash
make scheduler-resume
```

Resume enables future scheduled runs; it does not invoke the ingestor immediately.
Wait for an ingestor session to start before publishing, then run the simulator:

```bash
(cd scripts && bash run-simulator.sh)
make scheduler-pause          # after the simulator finishes
```

It is configured by env vars in that script, not by the query config. `SIM_SCALE_FACTOR` is CSV duration over desired real duration, and `SIM_RUNTIME` caps the run in real time. The ingestor must already be pulling when it starts: a paused Scheduler job means nothing drains the subscription, and Pub/Sub delivers the resulting backlog out of order.

## Pull-Based Ingestion

`ingestor-pull` (`src/ingestor/pull.go`, entry point `IngestPull`) is the only ingestor deployed. Rather than one invocation per Pub/Sub message, it is HTTP-triggered by Cloud Scheduler on a fixed interval. Each invocation keeps a Receive session open for up to `maxSessionDuration`, writes what it drains into `data:<source>`, and nudges windower's `ProcessWindows` directly (fire-and-forget) every `interval`.

Terminology: a **Tick** is one Scheduler-triggered invocation; a **Drain** is the repeated pulling inside it, bounded by the session deadline and ended early once the subscription goes quiet.

Sessions overlap by design. A Scheduler job will not start a run while its own previous attempt is open, so one job holds a session roughly `maxSessionDuration / 120s` of the time; several jobs on staggered schedules are what keep the subscription continuously pulled. Overlap is safe because the writes are idempotent `ZADD`s and the windower takes the minimum watermark across live instances.

Terraform creates and manages `ais-stream-pull` and the Scheduler jobs together
with the ingestor. Keep the subscription between runs; use `make scheduler-pause`
and `make scheduler-resume` to control ingestion. Messages published while
paused accumulate as backlog, so stop the simulator when pausing ingestion.

The `create-pull-subscription.sh` and `delete-pull-subscription.sh` scripts are
legacy helpers for manually managed deployments. Do not use them for a
Terraform-managed subscription.

The push ingestor (`IngestEvent` in `src/ingestor/main.go`) is retired but still compiles; it fell behind under load, where per-message invocation overhead dominated. The two were never deployed at the same time. Its entry point is left in place because `init()` runs for every entry point built from this source directory, so removing it is a separate change.

## Query Config

The ingestor and windower load the query/source definitions from GCS at startup.
The windower passes the selected query and source schema to the worker in each
request. The simulator uses environment variables; its source name and timestamp
settings must match the source definition in the query config.

Terraform seeds `gs://<project>-config/query-config.yaml` from
`env/query-config-reference.yaml` by default. The bucket, object name and initial
file can be overridden through Terraform variables. Run `make terraform-output`
to see the deployed `query_config` URI.

From the repository root, using the default bucket and object names:

```bash
gcloud storage cat "gs://${PROJECT}-config/query-config.yaml" --project="$PROJECT"
gcloud storage cp "gs://${PROJECT}-config/query-config.yaml" ./query-config.yaml --project="$PROJECT"
```

To edit and activate the config:

```bash
(cd scripts && ./update-config.sh)
```

For non-default settings, set `CONFIG_BUCKET` and `CONFIG_OBJECT` to match the
deployment. The script reads the deployed region from Terraform outputs, uploads
the config, and rolls the ingestor and windower so they reload it. Uploading the object alone leaves warm instances
using their previous config. Terraform ignores later changes to the seeded
object; editing `query_config_file` after deployment does not update it.

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
