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

# --- Redis / VPC: referenced only. Terraform never creates, modifies, or destroys
# these — they're pre-existing shared infrastructure. ---

variable "redis_host" {
  description = "Private IP of the shared Cloud Memorystore Redis instance."
  type        = string
  default     = "10.101.64.19"
}

variable "redis_port" {
  description = "Redis port."
  type        = string
  default     = "6379"
}

# Confirmed via `terraform providers schema -json` (provider hashicorp/google
# v5.45.2): google_cloudfunctions2_function.service_config has no direct-VPC-egress
# fields (no network/subnetwork/network_interfaces) — vpc_connector is the only VPC
# attachment mechanism this resource type supports, so that's what every managed
# function uses. Live functions currently use direct VPC egress instead (no
# connector), so expect a network-config diff on first plan — see terraform/README.md.
variable "vpc_connector_name" {
  description = "Name of the existing Serverless VPC Access connector (redis-eu-west3-connector), used as the VPC attachment for all managed functions."
  type        = string
  default     = "redis-eu-west3-connector"
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

# --- scheduler-task-queue ---
# memory/cpu/concurrency below are GCP auto-computed defaults, see windower note above.

variable "scheduler_task_queue_memory" {
  type    = string
  default = "256Mi"
}

variable "scheduler_task_queue_cpu" {
  type    = string
  default = "0.1666"
}

variable "scheduler_task_queue_max_instances" {
  type    = number
  default = 3
}

variable "scheduler_task_queue_concurrency" {
  type    = number
  default = 1
}

variable "tasks_queue_name" {
  description = "Cloud Tasks queue used by scheduler-task-queue to fan out windower triggers."
  type        = string
  default     = "faastreams-queue"
}
