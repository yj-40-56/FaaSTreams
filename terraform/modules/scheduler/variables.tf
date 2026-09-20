variable "project_id" {
  type = string
}

variable "region" {
  type = string
}

variable "schedules" {
  type = map(string)
}

variable "ingestor_pull_url" {
  type = string
}
