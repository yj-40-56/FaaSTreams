variable "env_name" {
  description = "Short name for this environment, used to namespace all GCP resources (e.g. 'demo', 'bench-30s'). Must be unique per concurrent deployment."
  type        = string
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

variable "window_size" {
  description = "Tumbling window duration in seconds applied to all coordinator queries. Key benchmark variable."
  type        = number
  default     = 60
}

# --- Worker ---

variable "worker_memory" {
  description = "Memory for the worker Cloud Function (e.g. '2048Mi', '4096Mi'). Key benchmark variable."
  type        = string
  default     = "2048Mi"
}

variable "worker_max_instances" {
  description = "Maximum concurrent instances of the worker function."
  type        = number
  default     = 10
}

variable "worker_timeout" {
  description = "Worker function timeout in seconds."
  type        = number
  default     = 540
}

# --- Coordinator ---

variable "coordinator_memory" {
  description = "Memory for the coordinator Cloud Function."
  type        = string
  default     = "1024Mi"
}

variable "coordinator_cpu" {
  description = "CPU count for the coordinator function (passed as string, e.g. '1')."
  type        = string
  default     = "1"
}

variable "coordinator_concurrency" {
  description = "Max concurrent requests per coordinator instance."
  type        = number
  default     = 100
}

variable "coordinator_max_instances" {
  description = "Maximum instances of the coordinator function."
  type        = number
  default     = 10
}

# --- Data Sink ---

variable "data_sink_memory" {
  description = "Memory for the data-sink Cloud Function."
  type        = string
  default     = "256Mi"
}

variable "data_sink_max_instances" {
  description = "Maximum instances of the data-sink function."
  type        = number
  default     = 10
}

# --- VPC ---

variable "vpc_connector_name" {
  description = "Name of the existing Serverless VPC Access connector to use for Redis access. Shared across all environments."
  type        = string
  default     = "redis-eu-west3-connector"
}
