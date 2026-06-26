resource "google_cloud_run_v2_service" "worker" {
  name     = "worker-${var.env_name}"
  location = var.region
  project  = var.project_id
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    timeout         = "${var.timeout}s"
    max_instance_request_concurrency = 1

    containers {
      image = var.image_uri

      ports {
        container_port = 8080
      }

      resources {
        limits = {
          memory = var.memory
          cpu    = "1"
        }
      }

      env {
        name  = "REDIS_HOST"
        value = var.redis_host
      }
      env {
        name  = "REDIS_PORT"
        value = var.redis_port
      }
      env {
        name  = "REDIS_KEY"
        value = var.redis_key
      }
      env {
        name  = "DATA_SINK_URL"
        value = var.data_sink_url
      }
    }

    scaling {
      max_instance_count = var.max_instances
    }

    vpc_access {
      connector = var.vpc_connector
      egress    = "PRIVATE_RANGES_ONLY"
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "public_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.worker.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
