output "url" {
  description = "HTTP trigger URL for the coordinator function."
  value       = google_cloudfunctions2_function.coordinator.url
}
