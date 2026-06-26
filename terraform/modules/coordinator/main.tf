locals {
  src_root        = "${path.root}/../src/coordinator"
  config_rendered = templatefile("${path.root}/templates/config.yaml.tpl", { window_size = var.window_size })
}

# Build source archive, injecting the templatefile-rendered config so the
# go:embed in config.go picks up the parameterized window sizes at Cloud Build time.
data "archive_file" "source" {
  type        = "zip"
  output_path = "${path.module}/coordinator-${var.env_name}.zip"

  source {
    content  = file("${local.src_root}/function.go")
    filename = "function.go"
  }
  source {
    content  = file("${local.src_root}/go.mod")
    filename = "go.mod"
  }
  source {
    content  = file("${local.src_root}/go.sum")
    filename = "go.sum"
  }
  source {
    content  = file("${local.src_root}/config/config.go")
    filename = "config/config.go"
  }
  source {
    content  = local.config_rendered
    filename = "config/config.yaml"
  }
  source {
    content  = file("${local.src_root}/internal/coordinatorcore/coordinator.go")
    filename = "internal/coordinatorcore/coordinator.go"
  }
  source {
    content  = file("${local.src_root}/internal/coordinatorcore/pubsub.go")
    filename = "internal/coordinatorcore/pubsub.go"
  }
  source {
    content  = file("${local.src_root}/internal/coordinatorcore/setup.go")
    filename = "internal/coordinatorcore/setup.go"
  }
}

resource "google_storage_bucket_object" "source" {
  name   = "coordinator-${var.env_name}-${data.archive_file.source.output_md5}.zip"
  bucket = var.source_bucket
  source = data.archive_file.source.output_path
}

resource "google_cloudfunctions2_function" "coordinator" {
  name     = "coordinator-${var.env_name}"
  location = var.region
  project  = var.project_id

  build_config {
    runtime     = "go126"
    entry_point = "Handler"
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
    timeout_seconds                  = 540
    ingress_settings                 = "ALLOW_ALL"
    all_traffic_on_latest_revision   = true
    vpc_connector                    = var.vpc_connector
    vpc_connector_egress_settings    = "PRIVATE_RANGES_ONLY"

    environment_variables = {
      RUN_MODE               = "http"
      REDIS_HOST             = var.redis_host
      REDIS_PORT             = var.redis_port
      REDIS_KEY              = var.redis_key
      COORDINATOR_KEY_PREFIX = var.coordinator_key_prefix
      WORKER_URL             = var.worker_url
      PUBSUB_PROJECT_ID      = var.project_id
      PUBSUB_TOPIC_ID        = var.topic_id
      PUBSUB_SUBSCRIPTION_ID = var.subscription_id
    }
  }
}

resource "google_cloudfunctions2_function_iam_member" "public_invoker" {
  project        = var.project_id
  location       = var.region
  cloud_function = google_cloudfunctions2_function.coordinator.name
  role           = "roles/cloudfunctions.invoker"
  member         = "allUsers"
}

resource "google_cloud_run_service_iam_member" "public_invoker" {
  project  = var.project_id
  location = var.region
  service  = google_cloudfunctions2_function.coordinator.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
