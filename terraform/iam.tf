# Dedicated accounts rather than the default compute one: organisations created
# since 2024 enforce iam.automaticIamGrantsForDefaultServiceAccounts, so on a
# fresh project the default account has no roles and neither builds nor runs.

resource "google_service_account" "build" {
  account_id   = "faastreams-build"
  display_name = "FaaSTreams function builds"

  depends_on = [time_sleep.apis_propagated]
}

resource "google_project_iam_member" "build_builder" {
  project = var.project_id
  role    = "roles/cloudbuild.builds.builder"
  member  = google_service_account.build.member
}

resource "google_service_account" "runtime" {
  account_id   = "faastreams-runtime"
  display_name = "FaaSTreams functions at runtime"

  depends_on = [time_sleep.apis_propagated]
}

# The ingestor and windower load the query config at startup.
resource "google_storage_bucket_iam_member" "runtime_reads_config" {
  bucket = google_storage_bucket.query_config.name
  role   = "roles/storage.objectViewer"
  member = google_service_account.runtime.member
}

resource "google_pubsub_subscription_iam_member" "runtime_pulls" {
  project      = var.project_id
  subscription = module.pubsub.subscription_id
  role         = "roles/pubsub.subscriber"
  member       = google_service_account.runtime.member
}

# New grants take a while to reach Cloud Build and Cloud Run.
resource "time_sleep" "iam_propagated" {
  create_duration = "60s"

  depends_on = [
    google_project_iam_member.build_builder,
    google_storage_bucket_iam_member.runtime_reads_config,
    google_pubsub_subscription_iam_member.runtime_pulls,
  ]
}
