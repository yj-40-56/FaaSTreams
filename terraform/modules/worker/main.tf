# Live worker is a plain Cloud Function deployed from source (see scripts/deploy-worker.sh),
# not a custom Docker image on Cloud Run — matching that here rather than reviving the
# earlier custom-image approach (decision: match live exactly).

data "archive_file" "source" {
  type        = "zip"
  source_dir  = "${path.root}/../src/worker"
  excludes    = ["__pycache__", "__pycache__/**"]
  output_path = "${path.module}/worker${var.name_suffix}.zip"
}

resource "google_storage_bucket_object" "source" {
  name   = "worker${var.name_suffix}-${data.archive_file.source.output_md5}.zip"
  bucket = var.source_bucket
  source = data.archive_file.source.output_path
}

resource "google_cloudfunctions2_function" "this" {
  name     = "worker${var.name_suffix}"
  location = var.region
  project  = var.project_id

  build_config {
    runtime     = "python312"
    entry_point = "handler"
    source {
      storage_source {
        bucket = var.source_bucket
        object = google_storage_bucket_object.source.name
      }
    }
  }

  service_config {
    available_memory                 = var.memory
    available_cpu                    = var.cpu
    max_instance_count               = var.max_instances
    max_instance_request_concurrency = var.concurrency
    timeout_seconds                  = var.timeout
    ingress_settings                 = "ALLOW_ALL"
    all_traffic_on_latest_revision   = true

    # See modules/ingestor_pull/main.tf for the direct-VPC-egress-vs-connector caveat.
    vpc_connector                 = var.vpc_connector
    vpc_connector_egress_settings = "PRIVATE_RANGES_ONLY"

    environment_variables = {
      DATA_SINK_URL = var.data_sink_url
      REDIS_HOST    = var.redis_host
      REDIS_PORT    = var.redis_port
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "public_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.this.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
