output "url" {
  description = "HTTP trigger URL for the pinger function."
  value       = google_cloudfunctions2_function.this.url
}
