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

variable "tasks_queue_name" {
  type = string
}

variable "windower_url" {
  type = string
}

variable "vpc_connector" {
  type = string
}

variable "source_bucket" {
  type = string
}
