locals {
  topic_id               = "ais-stream-${var.env_name}"
  subscription_id        = "ais-stream-${var.env_name}-sub"
  redis_key              = "mod-stream-${var.env_name}"
  coordinator_key_prefix = "coordinator-${var.env_name}"
  vpc_connector_id       = "projects/${var.project_id}/locations/${var.region}/connectors/${var.vpc_connector_name}"
}

resource "google_storage_bucket" "functions_source" {
  name          = "${var.project_id}-functions-source-${var.env_name}"
  location      = var.region
  force_destroy = true

  lifecycle_rule {
    condition { age = 7 }
    action { type = "Delete" }
  }
}

module "pubsub" {
  source          = "./modules/pubsub"
  project_id      = var.project_id
  topic_id        = local.topic_id
  subscription_id = local.subscription_id
  push_endpoint   = module.coordinator.url
}

module "data_sink" {
  source        = "./modules/data_sink"
  env_name      = var.env_name
  region        = var.region
  project_id    = var.project_id
  memory        = var.data_sink_memory
  max_instances = var.data_sink_max_instances
  redis_host    = var.redis_host
  redis_port    = var.redis_port
  redis_key     = "${local.redis_key}-results"
  vpc_connector = local.vpc_connector_id
  source_bucket = google_storage_bucket.functions_source.name
}

module "worker" {
  source        = "./modules/worker"
  env_name      = var.env_name
  region        = var.region
  project_id    = var.project_id
  image_uri     = "${var.region}-docker.pkg.dev/${var.project_id}/worker-images/worker-${var.env_name}"
  memory        = var.worker_memory
  max_instances = var.worker_max_instances
  timeout       = var.worker_timeout
  redis_host    = var.redis_host
  redis_port    = var.redis_port
  redis_key     = local.redis_key
  data_sink_url = module.data_sink.url
  vpc_connector = local.vpc_connector_id
}

module "coordinator" {
  source                 = "./modules/coordinator"
  env_name               = var.env_name
  region                 = var.region
  project_id             = var.project_id
  memory                 = var.coordinator_memory
  cpu                    = var.coordinator_cpu
  concurrency            = var.coordinator_concurrency
  max_instances          = var.coordinator_max_instances
  redis_host             = var.redis_host
  redis_port             = var.redis_port
  redis_key              = local.redis_key
  coordinator_key_prefix = local.coordinator_key_prefix
  worker_url             = module.worker.url
  topic_id               = local.topic_id
  subscription_id        = local.subscription_id
  window_size            = var.window_size
  vpc_connector          = local.vpc_connector_id
  source_bucket          = google_storage_bucket.functions_source.name
}
