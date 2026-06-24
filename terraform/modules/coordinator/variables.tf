variable "env_name" {
  type = string
}

variable "region" {
  type = string
}

variable "project_id" {
  type = string
}

variable "memory" {
  type = string
}

variable "cpu" {
  description = "CPU count as a string (e.g. '1', '2')."
  type        = string
}

variable "concurrency" {
  type = number
}

variable "max_instances" {
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

variable "coordinator_key_prefix" {
  type = string
}

variable "worker_url" {
  type = string
}

variable "topic_id" {
  type = string
}

variable "subscription_id" {
  type = string
}

variable "window_size" {
  type = number
}

variable "vpc_connector" {
  type = string
}

variable "source_bucket" {
  type = string
}
