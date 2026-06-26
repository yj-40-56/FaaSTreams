locals {
  src_root = "${path.root}/../src/worker"
}

data "archive_file" "source" {
  type        = "zip"
  output_path = "${path.module}/worker-${var.env_name}.zip"

  # Inject cloud-specific Dockerfile that pre-installs the DuckDB spatial
  # extension at build time. Cloud Build detects a Dockerfile in the source
  # zip and uses it instead of buildpacks.
  source {
    content  = file("${path.root}/docker/worker.Dockerfile")
    filename = "Dockerfile"
  }
  source {
    content  = file("${local.src_root}/requirements.txt")
    filename = "requirements.txt"
  }
  source {
    content  = file("${local.src_root}/handler.py")
    filename = "handler.py"
  }
  source {
    content  = file("${local.src_root}/analytics.py")
    filename = "analytics.py"
  }
  source {
    content  = file("${local.src_root}/fetch.py")
    filename = "fetch.py"
  }
  source {
    content  = file("${local.src_root}/zones.py")
    filename = "zones.py"
  }
  source {
    content  = file("${local.src_root}/main.py")
    filename = "main.py"
  }
}

resource "google_storage_bucket_object" "source" {
  name   = "worker-${var.env_name}-${data.archive_file.source.output_md5}.zip"
  bucket = var.source_bucket
  source = data.archive_file.source.output_path
}

resource "google_cloudfunctions2_function" "worker" {
  name     = "worker-${var.env_name}"
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
    available_memory               = var.memory
    max_instance_count             = var.max_instances
    timeout_seconds                = var.timeout
    ingress_settings               = "ALLOW_ALL"
    all_traffic_on_latest_revision = true
    vpc_connector                  = var.vpc_connector
    vpc_connector_egress_settings  = "PRIVATE_RANGES_ONLY"

    environment_variables = {
      REDIS_HOST    = var.redis_host
      REDIS_PORT    = var.redis_port
      REDIS_KEY     = var.redis_key
      DATA_SINK_URL = var.data_sink_url
    }
  }
}

resource "google_cloudfunctions2_function_iam_member" "public_invoker" {
  project        = var.project_id
  location       = var.region
  cloud_function = google_cloudfunctions2_function.worker.name
  role           = "roles/cloudfunctions.invoker"
  member         = "allUsers"
}

resource "google_cloud_run_service_iam_member" "public_invoker" {
  project  = var.project_id
  location = var.region
  service  = google_cloudfunctions2_function.worker.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
