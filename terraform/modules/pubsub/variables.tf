variable "project_id" {
  type = string
}

variable "topic_id" {
  type = string
}

variable "subscription_id" {
  type = string
}

variable "push_endpoint" {
  description = "Coordinator URL for Pub/Sub push delivery."
  type        = string
}
