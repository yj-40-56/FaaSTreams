# The deterministic cloudfunctions.net alias, which the services use to reach
# each other.
output "url" {
  value = google_cloudfunctions2_function.this.url
}
