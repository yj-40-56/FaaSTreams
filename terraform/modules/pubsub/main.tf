resource "google_pubsub_topic" "topic" {
  name    = var.topic_id
  project = var.project_id
}

resource "google_pubsub_subscription" "subscription" {
  name    = var.subscription_id
  topic   = google_pubsub_topic.topic.id
  project = var.project_id

  ack_deadline_seconds       = 600
  message_retention_duration = "600s"

  push_config {
    push_endpoint = var.push_endpoint
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}
