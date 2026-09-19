locals {
  query_config_bucket = coalesce(var.query_config_bucket, "${var.project_id}-config")

  redis_host = google_redis_instance.this.host
  redis_port = tostring(google_redis_instance.this.port)

  network = {
    network    = google_compute_network.vpc.name
    subnetwork = google_compute_subnetwork.subnet.name
  }

  identity = {
    build_service_account   = google_service_account.build.id
    runtime_service_account = google_service_account.runtime.email
  }
}

resource "google_storage_bucket" "functions_source" {
  name                        = "${var.project_id}-tf-functions-source"
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  depends_on = [time_sleep.apis_propagated]
}

module "pubsub" {
  source          = "./modules/pubsub"
  project_id      = var.project_id
  topic_id        = var.topic_id
  subscription_id = var.subscription_id

  depends_on = [time_sleep.apis_propagated]
}

module "data_sink" {
  source        = "./modules/function"
  name          = "data-sink"
  project_id    = var.project_id
  region        = var.region
  source_dir    = "${path.root}/../src/data-sink"
  source_bucket = google_storage_bucket.functions_source.name
  runtime       = "python312"
  entry_point   = "handler"
  sizing        = var.data_sink
  network       = local.network
  identity      = local.identity

  depends_on = [time_sleep.iam_propagated]
  environment = {
    REDIS_HOST = local.redis_host
    REDIS_PORT = local.redis_port
    REDIS_KEY  = "analytics-results"
  }
}

module "worker" {
  source        = "./modules/function"
  name          = "worker"
  project_id    = var.project_id
  region        = var.region
  source_dir    = "${path.root}/../src/worker"
  source_bucket = google_storage_bucket.functions_source.name
  runtime       = "python312"
  entry_point   = "handler"
  sizing        = var.worker
  network       = local.network
  identity      = local.identity

  depends_on = [time_sleep.iam_propagated]
  environment = {
    DATA_SINK_URL = module.data_sink.url
    REDIS_HOST    = local.redis_host
    REDIS_PORT    = local.redis_port
  }
}

module "windower" {
  source        = "./modules/function"
  name          = "windower"
  project_id    = var.project_id
  region        = var.region
  source_dir    = "${path.root}/../src/windower"
  source_bucket = google_storage_bucket.functions_source.name
  runtime       = "go126"
  entry_point   = "ProcessWindows"
  sizing        = var.windower
  network       = local.network
  identity      = local.identity

  depends_on = [time_sleep.iam_propagated]
  environment = {
    REDIS_URL     = "${local.redis_host}:${local.redis_port}"
    WORKER_URL    = module.worker.url
    CONFIG_BUCKET = google_storage_bucket.query_config.name
    CONFIG_OBJECT = google_storage_bucket_object.query_config.name
  }
}

module "ingestor_pull" {
  source        = "./modules/function"
  name          = "ingestor-pull"
  project_id    = var.project_id
  region        = var.region
  source_dir    = "${path.root}/../src/ingestor"
  source_bucket = google_storage_bucket.functions_source.name
  runtime       = "go126"
  entry_point   = "IngestPull"
  sizing        = var.ingestor_pull
  network       = local.network
  identity      = local.identity

  depends_on = [time_sleep.iam_propagated]
  environment = {
    REDIS_URL                   = "${local.redis_host}:${local.redis_port}"
    CONFIG_BUCKET               = google_storage_bucket.query_config.name
    CONFIG_OBJECT               = google_storage_bucket_object.query_config.name
    PUBSUB_PROJECT_ID           = var.project_id
    PUBSUB_PULL_SUBSCRIPTION_ID = module.pubsub.subscription_id
    WINDOWER_URL                = module.windower.url
  }
}

module "scheduler" {
  source            = "./modules/scheduler"
  region            = var.region
  project_id        = var.project_id
  schedules         = var.ingestor_schedules
  ingestor_pull_url = module.ingestor_pull.url
}
