# Create jobs paused; operators control subsequent pause/resume via Makefile targets.
resource "google_cloud_scheduler_job" "ingestor_tick" {
  for_each = var.schedules

  name      = each.key
  project   = var.project_id
  region    = var.region
  schedule  = each.value
  time_zone = "Etc/UTC"
  paused    = true

  lifecycle {
    ignore_changes = [paused]
  }

  # Holds the tick open for the whole session, so Scheduler skips this job's
  # next run while it is still pulling.
  attempt_deadline = "180s"

  http_target {
    http_method = "POST"
    uri         = var.ingestor_pull_url
  }

  retry_config {
    retry_count          = 0
    max_retry_duration   = "0s"
    min_backoff_duration = "5s"
    max_backoff_duration = "3600s"
    max_doublings        = 5
  }
}
