output "url" {
  description = "HTTP trigger URL for the scheduler-task-queue function."
  value       = google_cloudfunctions2_function.this.url
}
