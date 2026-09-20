output "subscription_id" {
  description = "Short name of the pull subscription (PUBSUB_PULL_SUBSCRIPTION_ID)."
  value       = google_pubsub_subscription.ais_stream_pull.name
}
