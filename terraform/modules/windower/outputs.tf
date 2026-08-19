output "url" {
  description = "HTTP trigger URL for the windower function."
  value       = google_cloudfunctions2_function.this.url
}
