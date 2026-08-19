# Pre-existing live queue, imported (not created fresh) — see terraform/README.md's
# import procedure. Rate/retry limits below are copied from the live queue's current
# config so import shows zero diff; adjust deliberately, not as a side effect of import.

resource "google_cloud_tasks_queue" "faastreams_queue" {
  name     = "faastreams-queue${local.name_suffix}"
  project  = var.project_id
  location = var.region

  rate_limits {
    max_concurrent_dispatches = 1000
    max_dispatches_per_second = 500
  }

  retry_config {
    max_attempts  = 100
    min_backoff   = "0.1s"
    max_backoff   = "3600s"
    max_doublings = 16
  }
}
