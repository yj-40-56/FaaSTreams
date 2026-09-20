# Pull-only: a push subscription would feed the retired push ingestor, which
# writes the same Redis keys.

resource "google_pubsub_topic" "ais_stream" {
  name    = var.topic_id
  project = var.project_id
}

resource "google_pubsub_subscription" "ais_stream_pull" {
  name    = var.subscription_id
  topic   = google_pubsub_topic.ais_stream.id
  project = var.project_id

  ack_deadline_seconds = 10
}
