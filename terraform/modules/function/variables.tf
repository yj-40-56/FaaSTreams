variable "name" {
  type = string
}

variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "source_dir" {
  type = string
}

variable "source_bucket" {
  type = string
}

variable "runtime" {
  type = string
}

variable "entry_point" {
  type = string
}

variable "sizing" {
  type = object({
    memory        = string
    cpu           = string
    max_instances = number
    concurrency   = number
    timeout       = number
  })
}

variable "network" {
  type = object({
    network    = string
    subnetwork = string
  })
}

variable "environment" {
  type = map(string)
}

variable "identity" {
  description = "Build service account (full resource ID) and runtime service account (email)."
  type = object({
    build_service_account   = string
    runtime_service_account = string
  })
}
