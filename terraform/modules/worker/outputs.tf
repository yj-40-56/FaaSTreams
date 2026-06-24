output "url" {
  description = "HTTP trigger URL for the worker function."
  value       = google_cloudfunctions2_function.worker.url
}
