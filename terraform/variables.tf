variable "env_name" {
  description = "Environment name. 'live' (the default) manages the real, unsuffixed production resources (ingestor-pull, windower, worker, data-sink, ...). Any other value namespaces all resources with a '-<env_name>' suffix for an isolated sandbox."
  type        = string
  default     = "live"
}

variable "project_id" {
  description = "GCP project ID."
  type        = string
  default     = "faastreams"
}

variable "region" {
  description = "GCP region for all resources."
  type        = string
  default     = "europe-west3"
}

# --- Redis / VPC: managed here. Both are shared, pre-existing, singleton objects
# that predate this config — IMPORT them, never let Terraform create them fresh.
# See shared_infra.tf for the ownership model and terraform/README.md for the
# import procedure. There is deliberately no `redis_host`/`redis_port` variable
# any more: those are now read off the managed instance (main.tf's locals), so the
# pipeline can no longer be pointed at a Redis that Terraform doesn't know about. ---

# No default, on purpose. Terraform manages this instance, so a wrong or guessed
# name would not adopt the existing Redis — it would stand up a SECOND, billable
# one and leave the real pipeline's data behind on the old one. Confirm the real
# name first and set it in environments/live.tfvars:
#   gcloud redis instances list --project=faastreams --region=europe-west3
variable "redis_instance_name" {
  description = "Instance ID of the shared Memorystore Redis instance. No default: see the comment above before setting it."
  type        = string
}

variable "redis_tier" {
  description = "Memorystore service tier. Live is Basic/standalone (gcloud.md)."
  type        = string
  default     = "BASIC"
}

# UNVERIFIED against live — the project's billing is disabled as of 2026-09-11, so
# `gcloud redis instances describe` could not be run to confirm it. After import,
# `terraform plan` will show the real value as a diff; correct this default to
# match rather than applying the diff (applying it would resize live Redis).
variable "redis_memory_size_gb" {
  description = "Memorystore capacity in GB. UNVERIFIED against live — confirm after import."
  type        = number
  default     = 1
}

# UNVERIFIED against live, same reason as redis_memory_size_gb. Held in
# shared_infra.tf's ignore_changes so an import can't trigger a version upgrade
# (which would force replacement of the instance).
variable "redis_version" {
  description = "Memorystore Redis version, used only when creating from scratch. UNVERIFIED against live — confirm after import."
  type        = string
  default     = "REDIS_7_0"
}

variable "network_name" {
  description = "VPC network Redis and the connector attach to. Read-only (data source) — Terraform never manages the project's default network."
  type        = string
  default     = "default"
}

# The `google_cloudfunctions2_function.service_config` schema (provider
# hashicorp/google v5.45.2, confirmed via `terraform providers schema -json`) has no
# direct-VPC-egress fields at all — no network/subnetwork/network_interfaces — so a
# connector is the only VPC attachment mechanism available to the managed functions,
# and that's what every one of them uses.
variable "vpc_connector_name" {
  description = "Name of the shared Serverless VPC Access connector."
  type        = string
  default     = "redis-eu-west3-connector"
}

# Connector settings below were read from the live API on 2026-09-11 and match it
# exactly. ip_cidr_range forces replacement if changed — and replacing the connector
# severs every function from Redis — so treat it as fixed.
variable "vpc_connector_cidr" {
  description = "/28 the connector's instances occupy."
  type        = string
  default     = "10.8.0.0/28"
}

variable "vpc_connector_machine_type" {
  type    = string
  default = "e2-micro"
}

variable "vpc_connector_min_instances" {
  type    = number
  default = 2
}

variable "vpc_connector_max_instances" {
  type    = number
  default = 3
}

# --- Query config: referenced only, same reasoning as Redis/VPC. The GCS object's
# content is hand-edited by the team today and is NOT managed by Terraform, so a
# first apply can never silently overwrite live query config. ---

variable "query_config_bucket" {
  description = "GCS bucket holding the live query-config.yaml."
  type        = string
  default     = "faastreams-config"
}

variable "query_config_object" {
  description = "Object name of the live query config within query_config_bucket."
  type        = string
  default     = "query-config.yaml"
}

# --- ingestor-pull ---

variable "ingestor_pull_memory" {
  type    = string
  default = "1024Mi"
}

variable "ingestor_pull_cpu" {
  description = "CPU count as a string (e.g. '1', '2')."
  type        = string
  default     = "2"
}

variable "ingestor_pull_max_instances" {
  type    = number
  default = 2
}

variable "ingestor_pull_concurrency" {
  type    = number
  default = 1
}

variable "ingestor_pull_timeout" {
  type    = number
  default = 120
}

# --- windower ---
# NOTE: memory/cpu/concurrency/max_instances/timeout below are GCP's auto-computed
# defaults observed live (no deploy script sets them explicitly). A future memory
# bump without an explicit cpu/concurrency override could silently diverge from
# whatever GCP would auto-pick for the new memory tier.

variable "windower_memory" {
  type    = string
  default = "256Mi"
}

variable "windower_cpu" {
  type    = string
  default = "0.1666"
}

variable "windower_max_instances" {
  type    = number
  default = 3
}

variable "windower_concurrency" {
  type    = number
  default = 1
}

variable "windower_timeout" {
  type    = number
  default = 60
}

# --- worker ---
# Explicit variables since the team actively tunes these.

variable "worker_memory" {
  type    = string
  default = "2048Mi"
}

variable "worker_cpu" {
  type    = string
  default = "1"
}

variable "worker_max_instances" {
  type    = number
  default = 4
}

variable "worker_concurrency" {
  type    = number
  default = 8
}

variable "worker_timeout" {
  type    = number
  default = 540
}

# --- data-sink ---
# memory/cpu/concurrency below are GCP auto-computed defaults, see windower note above.

variable "data_sink_memory" {
  type    = string
  default = "256Mi"
}

variable "data_sink_cpu" {
  type    = string
  default = "0.1666"
}

variable "data_sink_max_instances" {
  type    = number
  default = 3
}

variable "data_sink_concurrency" {
  type    = number
  default = 1
}

# --- pinger ---
# Deployed live as "pinger" (renamed by hand from scheduler-task-queue on 2026-08-21
# — see terraform/README.md). memory/cpu/concurrency below are GCP auto-computed
# defaults, see windower note above.

variable "pinger_memory" {
  type    = string
  default = "256Mi"
}

variable "pinger_cpu" {
  type    = string
  default = "0.1666"
}

variable "pinger_max_instances" {
  type    = number
  default = 3
}

variable "pinger_concurrency" {
  type    = number
  default = 1
}

variable "tasks_queue_name" {
  description = "Cloud Tasks queue used by pinger to fan out windower triggers."
  type        = string
  default     = "faastreams-queue"
}
