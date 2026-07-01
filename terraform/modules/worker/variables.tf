variable "env_name" {
  type = string
}

variable "region" {
  type = string
}

variable "project_id" {
  type = string
}

variable "image_uri" {
  type        = string
  description = "Full Artifact Registry image URI for the worker. Build with: make build-worker ENV=<env>"
}

variable "memory" {
  type = string
}

variable "max_instances" {
  type = number
}

variable "timeout" {
  type = number
}

variable "redis_host" {
  type = string
}

variable "redis_port" {
  type = string
}

variable "redis_key" {
  type = string
}

variable "data_sink_url" {
  type = string
}

variable "vpc_connector" {
  type = string
}
