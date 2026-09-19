output "fanout_trigger_name" {
  value = google_cloud_scheduler_job.fanout_trigger.name
}

output "ingestor_pull_trigger_name" {
  value = google_cloud_scheduler_job.ingestor_pull_trigger.name
}
