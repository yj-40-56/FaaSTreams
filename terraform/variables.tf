variable "project_id" {
  description = "GCP project the pipeline runs in. Created and billed by hand; Terraform does not create the project."
  type        = string
}

variable "region" {
  type    = string
  default = "europe-west3"
}

# --- Infrastructure ---

variable "network_name" {
  type    = string
  default = "faastreams-vpc"
}

variable "subnet_name" {
  type    = string
  default = "faastreams-subnet"
}

# Direct VPC egress draws function instance IPs from this range, so it bounds
# how many instances can scale out across all functions at once.
variable "subnet_cidr" {
  type    = string
  default = "10.20.0.0/24"
}

variable "private_services_range_name" {
  type    = string
  default = "faastreams-psa-range"
}

variable "redis_instance_name" {
  type    = string
  default = "faastreams-redis"
}

variable "redis_tier" {
  type    = string
  default = "BASIC"
}

# Each 60s window's events sit here until the worker confirms it; 1.1M-row
# windows at scale 96 need the headroom.
variable "redis_memory_size_gb" {
  type    = number
  default = 5
}

variable "redis_version" {
  type    = string
  default = "REDIS_7_0"
}

variable "enable_redis_bastion" {
  description = "VM for reaching Redis's private IP through the redis_tunnel_cmd output."
  type        = bool
  default     = true
}

variable "bastion_allow_external_ssh" {
  description = "Open port 22 on the bastion to the internet for direct SSH. The redis_tunnel_cmd output uses IAP and does not need this."
  type        = bool
  default     = false
}

# --- Query config ---

variable "query_config_bucket" {
  description = "Defaults to <project_id>-config."
  type        = string
  default     = null
}

variable "query_config_object" {
  type    = string
  default = "query-config.yaml"
}

variable "query_config_file" {
  description = "Uploaded as the initial query config. Ignored once the object exists."
  type        = string
  default     = "../env/query-config-reference.yaml"
}

# --- Pub/Sub ---

variable "topic_id" {
  type    = string
  default = "ais-stream"
}

variable "subscription_id" {
  type    = string
  default = "ais-stream-pull"
}

# --- Cloud Scheduler ---

# Each job restarts an ingestor session (up to 100s), and Scheduler skips a run
# while the previous attempt is open, so one job yields a session every ~120s:
# J jobs give ~0.83J concurrent pullers. Four measured ~4 and kept up with
# 19,200 events/sec; two capped ingestion at 10,939.
variable "ingestor_schedules" {
  description = "Cloud Scheduler job name => cron. Every job POSTs ingestor-pull."
  type        = map(string)
  default = {
    "ingestor-tick-even"   = "*/2 * * * *"
    "ingestor-tick-even-2" = "*/2 * * * *"
    "ingestor-tick-odd"    = "1-59/2 * * * *"
    "ingestor-tick-odd-2"  = "1-59/2 * * * *"
  }
}

# --- Function sizing ---
# Budgeted against the region's ~20 vCPU shared at runtime, and at most 10
# instances each (the direct VPC egress cap).

variable "ingestor_pull" {
  type = object({
    memory        = string
    cpu           = string
    max_instances = number
    concurrency   = number
    timeout       = number
  })
  # Timeout must outlast maxSessionDuration (100s) plus the final pacing sleep.
  default = {
    memory        = "2048Mi"
    cpu           = "2"
    max_instances = 4
    concurrency   = 1
    timeout       = 180
  }
}

variable "windower" {
  type = object({
    memory        = string
    cpu           = string
    max_instances = number
    concurrency   = number
    timeout       = number
  })
  default = {
    memory        = "256Mi"
    cpu           = "0.1666"
    max_instances = 4
    concurrency   = 1
    timeout       = 60
  }
}

variable "worker" {
  type = object({
    memory        = string
    cpu           = string
    max_instances = number
    concurrency   = number
    timeout       = number
  })
  # 2,366Mi measured at ~1.1M rows per window. Concurrency stays 1: windows
  # sharing an instance share its memory limit.
  default = {
    memory        = "4096Mi"
    cpu           = "2"
    max_instances = 3
    concurrency   = 1
    timeout       = 540
  }
}

variable "data_sink" {
  type = object({
    memory        = string
    cpu           = string
    max_instances = number
    concurrency   = number
    timeout       = number
  })
  default = {
    memory        = "256Mi"
    cpu           = "0.1666"
    max_instances = 2
    concurrency   = 1
    timeout       = 60
  }
}
