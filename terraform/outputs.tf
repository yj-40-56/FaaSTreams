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

output "pinger_url" {
  description = "HTTP trigger URL for the pinger function."
  value       = module.pinger.url
}

output "pubsub_topic" {
  description = "Pub/Sub topic name (ais-stream)."
  value       = module.pubsub.topic_id
}

output "pubsub_subscription" {
  description = "Pub/Sub pull subscription name."
  value       = module.pubsub.subscription_id
}

output "redis_host" {
  description = "Private IP of the shared Memorystore Redis instance."
  value       = local.redis_host
}

output "redis_port" {
  description = "Port of the shared Memorystore Redis instance."
  value       = local.redis_port
}

output "vpc_connector" {
  description = "Serverless VPC Access connector all managed functions egress through."
  value       = local.vpc_connector_id
}

output "tasks_queue" {
  description = "Cloud Tasks queue pinger fans windower triggers out through."
  value       = google_cloud_tasks_queue.faastreams_queue.name
}

output "scheduler_jobs" {
  description = "Cloud Scheduler job names driving the pipeline."
  value = [
    module.scheduler.fanout_trigger_name,
    module.scheduler.ingestor_pull_trigger_name,
  ]
}
