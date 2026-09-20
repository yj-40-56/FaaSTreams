# FaaSTreams Terraform

Provisions the whole pipeline in one GCP project: the shared infrastructure it
runs on, the four services, and the Scheduler ticks that drive them.

```
Cloud Scheduler (4 jobs) -> ingestor-pull -> windower -> worker -> data-sink
                                  |               \________|________/
                            ais-stream-pull          Memorystore Redis
```

One stack per project, selected by `PROJECT`. It sets `project_id` and names the
state bucket (`<project>-terraform-state`), so a new project needs no files of
its own — see `DEPLOYING.md`. `environments/<project>.tfvars` is optional and
holds only what differs from the defaults in `variables.tf`: `faas-pj` (the
personal sandbox) overrides one setting, `faastreams-e2e-0919` (a from-scratch
deploy test) another.

The official `faastreams` project is not managed here: it was set up by hand on
the `default` network, and `scripts/benchmark.sh` still targets it.

## What it manages

| | Resources |
|---|---|
| APIs | the twelve services the pipeline needs |
| Network | `faastreams-vpc`, `faastreams-subnet`, the Private Services Access range and peering |
| Redis | `faastreams-redis`, BASIC, 5 GB |
| Bastion | `faastreams-redis-bastion` (redis-tools preinstalled) and its SSH firewall rules |
| Config | the `<project>-config` bucket, and `query-config.yaml` seeded once from `query_config_file` |
| Pub/Sub | the `ais-stream` topic and `ais-stream-pull` subscription |
| Services | `ingestor-pull`, `windower`, `worker`, `data-sink` |
| Scheduler | four `ingestor-tick-*` jobs |

Networking is **direct VPC egress** on every function. No Serverless VPC Access
connector.

The config object is seeded once and later content changes are ignored. From
the repository root, run `(cd scripts && ./update-config.sh)` to edit it and roll
the ingestor and windower so they reload it. Set `CONFIG_BUCKET` and `CONFIG_OBJECT` when they differ from the script's
defaults; the region is read from Terraform outputs. Uploading with
`gcloud storage cp` alone does not reload the config in warm instances.

## Scheduling: why four Scheduler jobs

The windower is not driven by Scheduler. Each ingestor session runs up to 100s,
publishes its watermark and calls the windower every 5s (`interval` in
`src/ingestor/pull.go`). Scheduler only (re)starts sessions.

Scheduler skips a job's run while its previous attempt is open, so one job yields
a session about every 120s: J jobs give ~0.83J concurrent pullers. Four jobs on
alternating minutes measured ~4 concurrent sessions and kept up with 19,200
events/sec; two capped ingestion at 10,939.

The earlier pinger + Cloud Tasks fan-out (12 triggers per minute) is not modelled.
It predates the in-session ticker and was never measured against it.

## Usage

Prerequisites: `python3`, `gcloud auth login`,
`gcloud auth application-default login`, and `terraform` (or Docker, which the
Makefile falls back to).

```bash
export PROJECT=faas-pj             # or pass PROJECT= on each command
make -C terraform bucket-init      # once per project: the state bucket
make -C terraform init
make -C terraform plan             # safe anytime
make -C terraform apply            # interactive confirm
make -C terraform scheduler-resume # new jobs start paused; resume before feeding
make -C terraform scheduler-pause  # pause when idle
```

`PROJECT=` selects the project. Terraform authenticates with the gcloud
application-default credentials, so switching projects on different accounts
means `gcloud auth application-default login` as the other account, then
`make init` again. The root `Makefile` wraps the common targets as
`make terraform-plan` etc.

New jobs are created with `paused = true`; `ignore_changes = [paused]` preserves
subsequent manual pause/resume choices. Existing jobs retain their current state
when this configuration is applied.

`PROJECT` has no default. Before state reads or operational commands, the
Makefile verifies the initialized backend bucket, prefix and default workspace.
Changing projects requires `make init PROJECT=<project>`. Direct Terraform CLI
commands bypass this guard.

Set `region` in the project's tfvars file. Scheduler commands and
`scripts/update-config.sh` read the deployed `region` output rather than a
separate environment setting. Existing stacks need an apply to record this new
output before using those commands. `STATE_BUCKET_LOCATION` controls only the
state bucket's location during `bucket-init` (default: `europe-west3`).

## Infrastructure ownership

Use Terraform to update the functions, subscription and other managed resources.
Manual deployments or subscription deletion can introduce drift that the next
`apply` reverts. Pause and resume ingestion with the Makefile targets; retain the
Terraform-managed subscription between runs.

## Things to know

- **Direct VPC egress caps each function at 10 instances.** Beyond that needs a
  connector. The subnet (`/24`) is also where instance IPs come from.
- **~20 vCPU are shared at runtime** across every function in the region. Default
  sizing is budgeted to ~17.
- **Every function is public** (`allUsers` invoker), as with `gcloud
  --allow-unauthenticated`. The worker runs the SQL it is sent.
- **`bastion_allow_external_ssh`** opens port 22 to the internet when enabled
  (default: false). The `redis_tunnel_cmd` output uses `--tunnel-through-iap`,
  which works with the IAP-only firewall rule.
