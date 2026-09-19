data "archive_file" "source" {
  type        = "zip"
  source_dir  = var.source_dir
  excludes    = ["__pycache__", "__pycache__/**", ".venv", ".venv/**"]
  output_path = "${path.root}/.archives/${var.name}.zip"
}

# Content-addressed, so a source change uploads a new object and redeploys.
resource "google_storage_bucket_object" "source" {
  name   = "${var.name}-${data.archive_file.source.output_md5}.zip"
  bucket = var.source_bucket
  source = data.archive_file.source.output_path
}

resource "google_cloudfunctions2_function" "this" {
  name     = var.name
  location = var.region
  project  = var.project_id

  build_config {
    runtime         = var.runtime
    entry_point     = var.entry_point
    service_account = var.identity.build_service_account
    source {
      storage_source {
        bucket = var.source_bucket
        object = google_storage_bucket_object.source.name
      }
    }
  }

  service_config {
    available_memory                 = var.sizing.memory
    available_cpu                    = var.sizing.cpu
    max_instance_count               = var.sizing.max_instances
    max_instance_request_concurrency = var.sizing.concurrency
    timeout_seconds                  = var.sizing.timeout
    ingress_settings                 = "ALLOW_ALL"
    all_traffic_on_latest_revision   = true
    service_account_email            = var.identity.runtime_service_account

    direct_vpc_network_interface {
      network    = var.network.network
      subnetwork = var.network.subnetwork
    }
    direct_vpc_egress = "VPC_EGRESS_PRIVATE_RANGES_ONLY"

    environment_variables = var.environment
  }
}

# The services call each other over plain HTTPS without auth.
resource "google_cloud_run_v2_service_iam_member" "public_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.this.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
