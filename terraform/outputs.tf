output "ingestor_pull_url" {
  description = "HTTP trigger URL for the ingestor-pull function."
  value       = module.ingestor_pull.url
}

output "windower_url" {
  description = "HTTP trigger URL for the windower function."
  value       = module.windower.url
}

output "worker_url" {
  description = "HTTP trigger URL for the worker function."
  value       = module.worker.url
}

output "data_sink_url" {
  description = "HTTP trigger URL for the data-sink function."
  value       = module.data_sink.url
}

output "scheduler_task_queue_url" {
  description = "HTTP trigger URL for the scheduler-task-queue function."
  value       = module.scheduler_task_queue.url
}

output "pubsub_topic" {
  description = "Pub/Sub topic name (ais-stream)."
  value       = module.pubsub.topic_id
}

output "pubsub_subscription" {
  description = "Pub/Sub pull subscription name."
  value       = module.pubsub.subscription_id
}
