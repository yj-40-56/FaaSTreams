locals {
  # "live" (the default) manages the real, unsuffixed production resources.
  # Any other env_name namespaces everything with a "-<env_name>" suffix instead.
  name_suffix = var.env_name == "live" ? "" : "-${var.env_name}"

  topic_id        = "ais-stream"
  subscription_id = "ais-stream-pull${local.name_suffix}"

  vpc_connector_id = "projects/${var.project_id}/locations/${var.region}/connectors/${var.vpc_connector_name}"
}

resource "google_storage_bucket" "functions_source" {
  name          = "${var.project_id}-tf-functions-source${local.name_suffix}"
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
}

module "data_sink" {
  source        = "./modules/data_sink"
  name_suffix   = local.name_suffix
  region        = var.region
  project_id    = var.project_id
  memory        = var.data_sink_memory
  cpu           = var.data_sink_cpu
  concurrency   = var.data_sink_concurrency
  max_instances = var.data_sink_max_instances
  redis_host    = var.redis_host
  redis_port    = var.redis_port
  redis_key     = "analytics-results"
  vpc_connector = local.vpc_connector_id
  source_bucket = google_storage_bucket.functions_source.name
}

module "worker" {
  source        = "./modules/worker"
  name_suffix   = local.name_suffix
  region        = var.region
  project_id    = var.project_id
  memory        = var.worker_memory
  cpu           = var.worker_cpu
  concurrency   = var.worker_concurrency
  max_instances = var.worker_max_instances
  timeout       = var.worker_timeout
  redis_host    = var.redis_host
  redis_port    = var.redis_port
  data_sink_url = module.data_sink.url
  vpc_connector = local.vpc_connector_id
  source_bucket = google_storage_bucket.functions_source.name
}

module "windower" {
  source              = "./modules/windower"
  name_suffix         = local.name_suffix
  region              = var.region
  project_id          = var.project_id
  memory              = var.windower_memory
  cpu                 = var.windower_cpu
  concurrency         = var.windower_concurrency
  max_instances       = var.windower_max_instances
  timeout             = var.windower_timeout
  redis_host          = var.redis_host
  redis_port          = var.redis_port
  query_config_bucket = var.query_config_bucket
  query_config_object = var.query_config_object
  worker_url          = module.worker.url
  vpc_connector       = local.vpc_connector_id
  source_bucket       = google_storage_bucket.functions_source.name
}

module "ingestor_pull" {
  source              = "./modules/ingestor_pull"
  name_suffix         = local.name_suffix
  region              = var.region
  project_id          = var.project_id
  memory              = var.ingestor_pull_memory
  cpu                 = var.ingestor_pull_cpu
  concurrency         = var.ingestor_pull_concurrency
  max_instances       = var.ingestor_pull_max_instances
  timeout             = var.ingestor_pull_timeout
  redis_host          = var.redis_host
  redis_port          = var.redis_port
  query_config_bucket = var.query_config_bucket
  query_config_object = var.query_config_object
  subscription_id     = module.pubsub.subscription_id
  windower_url        = module.windower.url
  vpc_connector       = local.vpc_connector_id
  source_bucket       = google_storage_bucket.functions_source.name
}

module "scheduler_task_queue" {
  source           = "./modules/scheduler_task_queue"
  name_suffix      = local.name_suffix
  region           = var.region
  project_id       = var.project_id
  memory           = var.scheduler_task_queue_memory
  cpu              = var.scheduler_task_queue_cpu
  concurrency      = var.scheduler_task_queue_concurrency
  max_instances    = var.scheduler_task_queue_max_instances
  tasks_queue_name = google_cloud_tasks_queue.faastreams_queue.name
  windower_url     = module.windower.url
  vpc_connector    = local.vpc_connector_id
  source_bucket    = google_storage_bucket.functions_source.name
}

module "scheduler" {
  source                   = "./modules/scheduler"
  name_suffix              = local.name_suffix
  project_id               = var.project_id
  region                   = var.region
  scheduler_task_queue_url = module.scheduler_task_queue.url
  ingestor_pull_url        = module.ingestor_pull.url
}
