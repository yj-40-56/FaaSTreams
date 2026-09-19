output "job_names" {
  value = [for job in google_cloud_scheduler_job.ingestor_tick : job.name]
}
