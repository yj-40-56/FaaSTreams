# Pull-only. The push ingestor and this pull subscription must never coexist
# (they write the same Redis keys) — this module intentionally has no push_config.

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
