data "archive_file" "source" {
  type        = "zip"
  source_dir  = "${path.root}/../src/data-sink"
  output_path = "${path.module}/data-sink${var.name_suffix}.zip"
}

resource "google_storage_bucket_object" "source" {
  name   = "data-sink${var.name_suffix}-${data.archive_file.source.output_md5}.zip"
  bucket = var.source_bucket
  source = data.archive_file.source.output_path
}

resource "google_cloudfunctions2_function" "this" {
  name     = "data-sink${var.name_suffix}"
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
    timeout_seconds                  = 60
    ingress_settings                 = "ALLOW_ALL"
    all_traffic_on_latest_revision   = true

    # See modules/ingestor_pull/main.tf for the direct-VPC-egress-vs-connector caveat.
    vpc_connector                 = var.vpc_connector
    vpc_connector_egress_settings = "PRIVATE_RANGES_ONLY"

    environment_variables = {
      REDIS_HOST = var.redis_host
      REDIS_PORT = var.redis_port
      REDIS_KEY  = var.redis_key
    }
  }

  # Attaching a VPC connector broke ingestor-pull/windower in production on
  # 2026-08-19 (fixed by hand via --clear-vpc-connector, see terraform/README.md);
  # applying the same caution here even though no fix was needed for data-sink yet —
  # defer to whatever's live instead of fighting it on every apply.
  lifecycle {
    ignore_changes = [
      service_config[0].vpc_connector,
      service_config[0].vpc_connector_egress_settings,
    ]
  }
}

resource "google_cloud_run_v2_service_iam_member" "public_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.this.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
