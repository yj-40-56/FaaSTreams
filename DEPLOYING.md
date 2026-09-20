# Deploying FaaSTreams

Deploying the whole pipeline into a fresh GCP project, assuming no Terraform
experience. `terraform/README.md` covers the stack itself; this is the walkthrough.

## What Terraform is doing here

`terraform/` describes the resources the pipeline needs — network, Redis,
Pub/Sub, the four functions, the Scheduler jobs. Terraform compares that
description against the project and makes the project match. Running it twice
changes nothing the second time, so `make terraform-plan` is safe at any point:
it only prints the difference.

To know which real resources it already created, Terraform keeps a **state
file**. That file lives in a GCS bucket inside the project it describes, named
`<project>-terraform-state`. The bucket can't be managed by the state it holds,
so creating it is a separate one-time step (`make terraform-bucket-init`).

`PROJECT` is the only knob. It sets `project_id`, names the state bucket, and
picks the optional `terraform/environments/<project>.tfvars`. A project that
wants the defaults needs no files of its own.

## Prerequisites

- **A GCP project with billing enabled.** Terraform does not create the project;
  it needs one to exist before it can put anything in it.
- **`gcloud`**, authenticated twice — once for the CLI, once for Terraform:
  ```bash
  gcloud auth login
  gcloud auth application-default login
  ```
  Terraform uses the application-default credentials, so they must belong to an
  account with access to the target project.
- **`terraform`** — optional. Without it the Makefile falls back to the
  `hashicorp/terraform` Docker image automatically.
- **`python3`** — for the local project/backend guard and Scheduler commands.
- **`go`** — only to run the simulator.
- **The AIS CSV** at `data/ais.csv`. It is not in the repo (`.gitignore` excludes
  `data/`), and it is ~3 GB. Ask for it separately.

Quota: the sizing assumes ~20 vCPU available in `europe-west3` and a 5 GB
Memorystore instance. A billing-enabled project normally has room; a trial
account may not.

## Deploy

Run the commands below from the repository root.

```bash
export PROJECT=your-project-id        # everything below reads this

make terraform-bucket-init            # once per project: the state bucket
make terraform-init                   # connect to it, fetch the provider
make terraform-plan                   # review: ~40 resources, all additions
make terraform-apply                  # confirm at the prompt
```

`export` once per terminal is the least error-prone; `make terraform-plan
PROJECT=...` on each command works identically.

First apply takes roughly ten minutes. Most of it is enabling APIs (with a
deliberate 60s settle), the Private Services Access peering, and the Redis
instance.

New Scheduler jobs are created **paused**. Resume them when ready to feed the
pipeline. Terraform ignores subsequent changes to their paused state, so an
apply preserves manual pause/resume choices, including for existing jobs.

Check what you got:

```bash
make terraform-output                 # service URLs, Redis host, job names
```

## Run it

```bash
make scheduler-resume
```

This enables all four jobs at their next scheduled times; it does not invoke the
ingestor immediately. Wait for an ingestor session to start before publishing:

```bash
(cd scripts && ./run-simulator.sh)      # reads PROJECT; blocks for SIM_RUNTIME
```

`SIM_RUNTIME`, `SIM_SCALE_FACTOR` and `SIM_CSV_PATH` override the defaults
(5m, 24, `../../data/ais.csv`).

Results land in the data-sink. To change the query, edit the config in place:

```bash
(cd scripts && ./update-config.sh)      # opens $EDITOR, uploads, rolls both services
```

Pause the ticks again when you stop:

```bash
make scheduler-pause
```

## Tearing down

```bash
make -C terraform destroy             # prompts for the project ID
```

This removes everything including Redis and its contents. The state bucket
survives; delete it with the project.

## Per-project settings

Only for values that differ from `terraform/variables.tf`. Create
`terraform/environments/<project>.tfvars` and set just those:

```hcl
query_config_file = "../env/query-config-sliding-30.yaml" # alternate initial query
redis_memory_size_gb = 2                               # smaller Redis
enable_redis_bastion = false                           # skip the bastion VM
region = "europe-west1"                               # optional deployment region
```

Do not set `project_id` there — it comes from `PROJECT`. Scheduler commands and
`update-config.sh` read the deployed region from Terraform outputs. After
upgrading an existing stack to include the `region` output, run
`make terraform-apply` before using those commands.

The state bucket location is independent of the deployment region. Override it
only when creating the bucket, for example:
`make terraform-bucket-init STATE_BUCKET_LOCATION=europe-west1`.

## Two things that will bite

**Use Terraform for infrastructure changes.** Manual function deployments and
subscription creation/deletion can drift from Terraform's configuration; a later
`apply` can revert those changes. Query config content is intentionally managed
outside Terraform after its initial upload; use `scripts/update-config.sh` to
edit it and roll the services that load it.

**Select the project explicitly.** `PROJECT` has no default. Export it once per
terminal, and run `make terraform-init` whenever switching projects. Makefile
targets check that the initialized GCS backend matches the selected project and
uses the default Terraform workspace before reading state or operating on the
stack. A mismatch stops the command with instructions to reinitialize. Use the
Makefile targets to retain this guard; direct Terraform CLI commands bypass it.
