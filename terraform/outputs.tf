output "region" {
  description = "Deployed region used by operational commands."
  value       = var.region
}

output "ingestor_pull_url" {
  value = module.ingestor_pull.url
}

output "windower_url" {
  value = module.windower.url
}

output "worker_url" {
  value = module.worker.url
}

output "data_sink_url" {
  value = module.data_sink.url
}

output "pubsub_subscription" {
  value = module.pubsub.subscription_id
}

output "query_config" {
  value = "gs://${google_storage_bucket.query_config.name}/${google_storage_bucket_object.query_config.name}"
}

output "redis_host" {
  value = local.redis_host
}

output "scheduler_jobs" {
  value = module.scheduler.job_names
}

output "redis_tunnel_cmd" {
  description = "Then, in another terminal: redis-cli -h localhost -p 6379 ping"
  value = var.enable_redis_bastion ? join(" ", [
    "gcloud compute ssh", google_compute_instance.redis_bastion[0].name,
    "--zone=${google_compute_instance.redis_bastion[0].zone} --project=${var.project_id} --tunnel-through-iap --",
    "-N -L 6379:${local.redis_host}:${local.redis_port}",
  ]) : null
}
