# google_cloud_scheduler_job has no pause/enabled attribute in the provider — pause
# state is an imperative `gcloud scheduler jobs pause|resume` action, not something
# Terraform declares. See terraform/Makefile's scheduler-pause/scheduler-resume targets.
#
# Job names are preserved as-is even though they're confusingly named (the "5sec"
# job actually runs every minute and its 5s cadence comes from scheduler-task-queue's
# internal Cloud Tasks fan-out; "windower-tick" actually targets ingestor-pull) —
# renaming requires destroy+recreate in GCP and wasn't part of this change.

resource "google_cloud_scheduler_job" "fanout_trigger" {
  name      = "coordinator-5sec-trigger${var.name_suffix}"
  project   = var.project_id
  region    = var.region
  schedule  = "* * * * *"
  time_zone = "Europe/Amsterdam"

  http_target {
    http_method = "POST"
    uri         = var.scheduler_task_queue_url
  }

  attempt_deadline = "180s"

  # Matches the live jobs' current (provider-default) retry config — declared
  # explicitly so plan doesn't try to strip it back to null on apply.
  retry_config {
    retry_count          = 0
    max_retry_duration   = "0s"
    min_backoff_duration = "5s"
    max_backoff_duration = "3600s"
    max_doublings        = 5
  }
}

resource "google_cloud_scheduler_job" "ingestor_pull_trigger" {
  name      = "windower-tick${var.name_suffix}"
  project   = var.project_id
  region    = var.region
  schedule  = "* * * * *"
  time_zone = "Etc/UTC"

  http_target {
    http_method = "GET"
    uri         = var.ingestor_pull_url
  }

  attempt_deadline = "180s"

  # Matches the live jobs' current (provider-default) retry config — declared
  # explicitly so plan doesn't try to strip it back to null on apply.
  retry_config {
    retry_count          = 0
    max_retry_duration   = "0s"
    min_backoff_duration = "5s"
    max_backoff_duration = "3600s"
    max_doublings        = 5
  }
}
