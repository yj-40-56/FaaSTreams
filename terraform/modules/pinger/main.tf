# Deployed as "pinger" (matching the historically-intended name, restored by hand
# outside Terraform on 2026-08-21 — see terraform/README.md's "pinger" note). The
# source code is unchanged: it's still src/scheduler_task_queue's
# windower_sub_1_trigger, just deployed under a different function name.
data "archive_file" "source" {
  type        = "zip"
  source_dir  = "${path.root}/../src/scheduler_task_queue"
  output_path = "${path.module}/pinger${var.name_suffix}.zip"
}

resource "google_storage_bucket_object" "source" {
  name   = "pinger${var.name_suffix}-${data.archive_file.source.output_md5}.zip"
  bucket = var.source_bucket
  source = data.archive_file.source.output_path
}

resource "google_cloudfunctions2_function" "this" {
  name     = "pinger${var.name_suffix}"
  location = var.region
  project  = var.project_id

  build_config {
    runtime     = "python312"
    entry_point = "windower_sub_1_trigger"
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
      GCP_PROJECT  = var.project_id
      GCP_REGION   = var.region
      TASKS_QUEUE  = var.tasks_queue_name
      WINDOWER_URL = var.windower_url
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
