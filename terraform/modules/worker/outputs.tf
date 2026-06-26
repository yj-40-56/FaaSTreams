output "url" {
  description = "URL of the worker Cloud Run service."
  value       = google_cloud_run_v2_service.worker.uri
}
