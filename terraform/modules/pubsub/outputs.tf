output "topic_id" {
  description = "Short name of the Pub/Sub topic (used directly in PUBSUB_TOPIC_ID env var)."
  value       = google_pubsub_topic.topic.name
}

output "subscription_id" {
  description = "Short name of the Pub/Sub subscription (used in PUBSUB_SUBSCRIPTION_ID env var)."
  value       = google_pubsub_subscription.subscription.name
}
