# FaaSTreams Terraform

Provisions the whole pipeline in one GCP project: the shared infrastructure it
runs on, the four services, and the Scheduler ticks that drive them.

```
Cloud Scheduler (4 jobs) -> ingestor-pull -> windower -> worker -> data-sink
                                  |               \________|________/
                            ais-stream-pull          Memorystore Redis
```

One stack per project. `faas-pj` (the personal sandbox) is the only one defined
today: `environments/faas-pj.tfvars` + `faas-pj.backend.hcl`. The official
`faastreams` project would be another such pair; it was set up by hand on the
`default` network and is not described here yet.

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

The config object is seeded once and then ignored, so `deploy-sandbox.sh
--config` or `gcloud storage cp` can swap it per run without Terraform reverting
it.

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

Prerequisites: `terraform` (or Docker, which the Makefile falls back to) and
`gcloud auth application-default login`.

```bash
make -C terraform bucket-init      # once per project: the state bucket
make -C terraform init
make -C terraform plan             # safe anytime
make -C terraform apply            # interactive confirm
make -C terraform scheduler-pause  # jobs start enabled; pause when idle
make -C terraform scheduler-resume # before feeding it
```

`PROJECT=` selects the project. Terraform authenticates with the gcloud
application-default credentials, so switching projects on different accounts
means `gcloud auth application-default login` as the other account, then
`make init` again. The root `Makefile` wraps the common targets as
`make terraform-plan` etc.

Pause state is not declarable in the provider: new jobs are created enabled and
an apply never touches an existing job's state.

## Running it next to deploy-sandbox.sh

`deploy-sandbox.sh` deploys the same functions with `gcloud`. Use one or the
other: the script's deploys show up in the next `plan` as drift to revert. Its
`--clean` (subscription reset, Redis `FLUSHALL`) is still useful on its own.

## Things to know

- **Direct VPC egress caps each function at 10 instances.** Beyond that needs a
  connector. The subnet (`/24`) is also where instance IPs come from.
- **~20 vCPU are shared at runtime** across every function in the region. Default
  sizing is budgeted to ~17.
- **Every function is public** (`allUsers` invoker), as with `gcloud
  --allow-unauthenticated`. The worker runs the SQL it is sent.
- **`bastion_allow_external_ssh`** opens port 22 to the internet, because
  `deploy-sandbox.sh` uses plain `gcloud compute ssh`. With
  `--tunnel-through-iap` the IAP-only rule is enough and this can go.
