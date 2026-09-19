output "url" {
  description = "HTTP trigger URL for the ingestor-pull function."
  value       = google_cloudfunctions2_function.this.url
}
