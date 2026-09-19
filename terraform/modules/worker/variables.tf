variable "name_suffix" {
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
  type = string
}

variable "concurrency" {
  type = number
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

variable "data_sink_url" {
  type = string
}

variable "vpc_connector" {
  type = string
}

variable "source_bucket" {
  type = string
}
