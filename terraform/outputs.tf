output "coordinator_url" {
  description = "HTTP trigger URL for the coordinator function."
  value       = module.coordinator.url
}

output "worker_url" {
  description = "HTTP trigger URL for the worker function."
  value       = module.worker.url
}

output "data_sink_url" {b
  description = "HTTP trigger URL for the data-sink function."
  value       = module.data_sink.url
}

output "pubsub_topic" {
  description = "Pub/Sub topic name for this environment."
  value       = module.pubsub.topic_id
}

output "pubsub_subscription" {
  description = "Pub/Sub subscription name for this environment."
  value       = module.pubsub.subscription_id
}
