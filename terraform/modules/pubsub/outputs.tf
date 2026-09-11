output "topic_id" {
  description = "Short name of the Pub/Sub topic (used directly as the ais-stream topic name)."
  value       = google_pubsub_topic.ais_stream.name
}

output "subscription_id" {
  description = "Short name of the Pub/Sub pull subscription (used in PUBSUB_PULL_SUBSCRIPTION_ID env var)."
  value       = google_pubsub_subscription.ais_stream_pull.name
}
