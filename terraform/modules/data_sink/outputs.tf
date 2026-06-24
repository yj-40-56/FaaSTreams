output "url" {
  description = "HTTP trigger URL for the data-sink function."
  value       = google_cloudfunctions2_function.data_sink.url
}
