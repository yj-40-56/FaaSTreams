# FaaSTreams Terraform

**Scope: infrastructure only.** Terraform stands up everything the pipeline runs
on; it does not run benchmarks. Benchmark orchestration lives in the repo-root
`Makefile` and `scripts/` (`make benchmark`, `scripts/save-results.py`) and is
deliberately kept out of this directory.

What it manages:

| | |
|---|---|
| Services | `ingestor-pull` -> `windower` -> `worker` -> `data-sink`, plus `pinger` |
| Redis | the shared Memorystore instance every service reads/writes |
| VPC | the `redis-eu-west3-connector` Serverless VPC Access connector the services egress through |
| Pub/Sub | the `ais-stream` topic and `ais-stream-pull` subscription |
| Cloud Tasks | the `faastreams-queue` that `pinger` fans out through |
| Cloud Scheduler | `coordinator-5sec-trigger` and `windower-tick` |
| Storage | the `-tf-functions-source` bucket holding zipped function sources |

**Note on `pinger`**: this module (`terraform/modules/pinger`) deploys
`src/scheduler_task_queue`'s code (entry point `windower_sub_1_trigger`) under the
function name `pinger`. It was originally modeled here as `scheduler-task-queue`
(the name it happened to be live under at the time), but on 2026-08-21 a teammate
manually deleted `scheduler-task-queue` and redeployed the same code as `pinger`,
repointing the live `coordinator-5sec-trigger` scheduler job at it — all outside
Terraform. The module was renamed to match; `terraform import` was re-run against
the `pinger` function/IAM binding to bring state back in sync. If this function
gets renamed again by hand, expect `terraform plan` to show the function as
missing (404) and want to recreate it under the old name — check Cloud Audit Logs
(`protoPayload.methodName` on `resource.type="cloud_function"`) before assuming
it's actually gone, the way this one wasn't.

## Shared infrastructure: Redis and the VPC connector

Redis and the VPC connector (`terraform/shared_infra.tf`) are **singletons** — one
of each exists for the whole project, and every environment shares them rather than
cloning them. Sandbox envs already shared live's Redis in practice (every env
pointed at the same hardcoded IP); cloning a Memorystore instance per sandbox would
bill real money to isolate something Redis key prefixes already isolate.

Only the `live` workspace declares them as resources; other workspaces read the same
objects through data sources, so a sandbox apply can't fight live over ownership.
Both carry `prevent_destroy = true` — destroying the Redis wipes every window and
result the pipeline has accumulated, and destroying the connector severs all five
functions from Redis at once.

They predate this config and were created by hand, so **they must be imported, never
created fresh** — see the import procedure below. The project's `default` VPC network
is *not* managed (it's shared with the `redis-bastion` VM and the hand-deployed push
ingestor); Terraform reads it through a data source.

**Push and pull ingestors must never run simultaneously** — they write the same
Redis keys. This repo's live pipeline uses the pull ingestor (`ingestor-pull`); the
old push ingestor (`src/ingestor`, entry point `IngestEvent`) is not managed by
Terraform and should not be deployed while `ingestor-pull` is live.

## Prerequisites

- Docker (for running Terraform — `terraform/Makefile` wraps `hashicorp/terraform:1.9`)
- `gcloud` CLI authenticated (`gcloud auth application-default login`)
- Network access to `registry.terraform.io` to fetch the `google`/`archive`
  providers on `init`. **Some networks geo-block this host** (confirmed via
  `curl -I https://registry.terraform.io/.well-known/terraform.json` returning
  `x-amzn-waf-reason: geo` and a 404) — if `terraform init` fails with "Invalid
  provider registry host", check for that header; a VPN out of the blocked region
  resolves it.
- GCS state bucket exists (`make -C terraform bucket-init` if not)
- **Billing enabled on the `faastreams` project.** As of 2026-09-11 it is *not*:
  `gcloud redis instances list` and `gcloud functions list` both fail with
  `BILLING_DISABLED` / "please check billing account associated". Nothing in this
  directory can `plan`, `apply`, or `import` until that is restored, and the live
  resources' current state cannot be read back to confirm this config matches them.

## First-time setup: importing the live pipeline

Terraform's state currently tracks nothing real (verified: `baseline`/`benchmark-*`
workspaces are empty, `default` workspace only has harmless `archive_file` hashes).
The live resources listed above already exist in GCP, deployed by hand — **do not
`apply` before importing them**, or Terraform will try to create duplicates and
fail on "already exists" (or worse, succeed and orphan the real ones).

First, set the Redis instance's real ID — `import-live` cannot run without it:

```bash
gcloud redis instances list --project=faastreams --region=europe-west3
```

and put that ID in `environments/live.tfvars` as `redis_instance_name`. There is no
default for it on purpose: Terraform now manages the Memorystore instance, so a
guessed name would not adopt the existing Redis — it would stand up a **second,
billable** one and strand the pipeline's data on the old one. `make import-redis`
refuses to run until the variable is set.

```bash
cd terraform
make init
make import-live      # imports every live resource into the `live` workspace, one at a
                      # time — including the VPC connector, and Redis via import-redis
make plan ENV=live    # MUST show only new-resource additions (functions_source
                       # bucket + its objects, IAM bindings) — never an unexplained
                       # update/destroy on anything just imported. If you see one,
                       # stop and fix the .tf default that doesn't match live before
                       # proceeding.
```

### Redis attributes this config had to guess

`redis_memory_size_gb` and `redis_version` could not be read back from the live
instance (billing disabled, see Prerequisites), so their defaults in `variables.tf`
are **unverified**. After import, `plan` will show the real values as a diff —
**correct the defaults to match live rather than applying the diff**, since applying
it would resize or upgrade the running Redis. `redis_version` and `reserved_ip_range`
are held in `ignore_changes` (both force replacement of the instance) until the real
values are confirmed and written in.

Only after a clean `plan` review:

```bash
make apply ENV=live   # interactive confirm (no -auto-approve is ever passed)
```

### Known first-plan diff: VPC egress

Live functions currently use **direct VPC egress** (`--network default --subnet
.../default`), not the `redis-eu-west3-connector` Serverless VPC Access connector
(which this config now manages — see Shared infrastructure above). Confirmed via `terraform providers schema -json` against provider
`hashicorp/google` v5.45.2: `google_cloudfunctions2_function.service_config` has no
direct-VPC-egress fields at all (no `network`/`subnetwork`/`network_interfaces`) —
`vpc_connector`/`vpc_connector_egress_settings` is the only VPC attachment mechanism
this resource type supports, so that's what this config uses for every managed
function. **`terraform plan` will show a network-config change on the four
functions + pinger on first plan** — this is expected and, given the
provider's limitations, unavoidable without switching resource types. Review it
before applying so you understand it's attaching the connector, not a sign of
something wrong with the config.

## Day-to-day

```bash
make -C terraform plan ENV=live      # preview changes, safe anytime
make -C terraform apply ENV=live     # apply after reviewing the plan
make -C terraform validate ENV=live
```

From the repo root, `make terraform-plan` / `make terraform-apply` wrap these (see
the root `README.md` / `Makefile`).

## Scheduler pause/resume

`google_cloud_scheduler_job` has no pause/enabled field in the provider — pausing a
job is an imperative `gcloud scheduler jobs pause|resume` action, not something
`terraform apply` can express or track.

```bash
make -C terraform scheduler-pause    # pauses both live jobs
make -C terraform scheduler-resume   # resumes only coordinator-5sec-trigger by default;
                                      # JOBS="coordinator-5sec-trigger windower-tick" to resume both
```

`windower-tick` (which targets `ingestor-pull`) is paused live today; `scheduler-resume`
deliberately does not re-enable it, since that's an operational decision, not a side
effect of running a benchmark. `make benchmark` (repo root) uses `scheduler-pause`/
`-resume` around each run automatically, and triggers `ingestor-pull` directly via
HTTP instead of relying on `windower-tick`.

## Saving benchmark results

Moved out of this directory — it is benchmark tooling, not infrastructure:

```bash
make save-results ENV=live      # from the repo root; writes results/{env}_{timestamp}.json
```

For ad hoc log inspection:

```bash
gcloud logging read \
  'resource.type="cloud_run_revision" AND (resource.labels.service_name="windower" OR resource.labels.service_name="worker") AND textPayload!=""' \
  --project=faastreams --limit=50 --format="value(timestamp,textPayload)" \
  --freshness=5m
```

## Query config

`query_config_bucket`/`query_config_object` (defaults: `faastreams-config` /
`query-config.yaml`) are referenced by variable only — Terraform does not manage
the object's content. The bucket has several hand-edited snapshot files today
(`query-config-sidar.yaml`, `query-config.pre-tdrive-20260721.yaml`, ...), so a
Terraform-owned object would risk silently overwriting live query config with
whatever's checked into the repo (nothing, currently) on first apply. Use
`scripts/update-config.sh` (repo root) to edit it by hand — it redeploys
`ingestor-pull` and `windower` afterward.

A future path to GitOps-manage this file: snapshot the live object into
`terraform/files/query-config.yaml`, add a `google_storage_bucket_object` resource,
import the existing object, and verify `terraform plan` shows zero diff before ever
applying.

## Isolated sandbox environments

The old `baseline`/`benchmark-*` tfvars (window-size and coordinator-memory
sweeps) were removed — they targeted variables (`window_size`, `coordinator_*`)
that no longer exist under this module set, and cloning the full 5-function live
pipeline per sandbox is meaningfully more expensive than the old 3-resource
coordinator setup. To stand up an isolated copy for an experiment, copy
`environments/live.tfvars` to `environments/<name>.tfvars`, set
`env_name = "<name>"`, and `make -C terraform apply ENV=<name>` — this creates a
fully separate, `-<name>`-suffixed set of resources that never touches live.
`make -C terraform purge-env ENV=<name>` cleans one up (refuses to run against
`ENV=live`).

## Other targets

| Command | Description |
|---------|-------------|
| `make -C terraform plan-check ENV=live` | `terraform plan -detailed-exitcode`, used by `make benchmark`'s drift guard |
| `make -C terraform purge-env ENV=<name>` | Delete orphaned GCP resources for a non-live sandbox env not in Terraform state |
| `make -C terraform destroy ENV=<name>` | Destroy an environment's resources. No root-level wrapper exists for this — deliberately, since `ENV` defaults to `live`. Redis and the VPC connector survive it (`prevent_destroy`). |
| `make -C terraform import-redis` | Adopt the existing Memorystore instance into state. Needs `redis_instance_name` set first; refuses to guess. |
