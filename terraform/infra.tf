# Stateful: Redis holds every in-flight window, and the network carries every
# function's route to it.

locals {
  apis = toset([
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "cloudfunctions.googleapis.com",
    "cloudscheduler.googleapis.com",
    "compute.googleapis.com",
    "eventarc.googleapis.com",
    "iam.googleapis.com",
    "pubsub.googleapis.com",
    "redis.googleapis.com",
    "run.googleapis.com",
    "servicenetworking.googleapis.com",
    "storage.googleapis.com",
  ])
}

resource "google_project_service" "apis" {
  for_each = local.apis

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# A freshly enabled API can reject calls for a while after enablement returns.
resource "time_sleep" "apis_propagated" {
  create_duration = "60s"

  triggers = {
    apis = join(",", sort([for api in google_project_service.apis : api.service]))
  }
}

resource "google_compute_network" "vpc" {
  name                    = var.network_name
  auto_create_subnetworks = false

  depends_on = [time_sleep.apis_propagated]
}

resource "google_compute_subnetwork" "subnet" {
  name                     = var.subnet_name
  region                   = var.region
  network                  = google_compute_network.vpc.id
  ip_cidr_range            = var.subnet_cidr
  private_ip_google_access = true
}

# Memorystore reaches the VPC only through Private Services Access peering.
resource "google_compute_global_address" "private_services_range" {
  name          = var.private_services_range_name
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 20
  network       = google_compute_network.vpc.id
}

resource "google_service_networking_connection" "private_services" {
  network                 = google_compute_network.vpc.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_services_range.name]
}

resource "google_redis_instance" "this" {
  name           = var.redis_instance_name
  region         = var.region
  tier           = var.redis_tier
  memory_size_gb = var.redis_memory_size_gb
  redis_version  = var.redis_version

  authorized_network = google_compute_network.vpc.id
  connect_mode       = "PRIVATE_SERVICE_ACCESS"

  depends_on = [google_service_networking_connection.private_services]
}

resource "google_storage_bucket" "query_config" {
  name                        = local.query_config_bucket
  location                    = var.region
  uniform_bucket_level_access = true

  depends_on = [time_sleep.apis_propagated]
}

# Seeds the object once so the ingestor and windower can start on a fresh
# project; later config uploads are not reverted.
resource "google_storage_bucket_object" "query_config" {
  name   = var.query_config_object
  bucket = google_storage_bucket.query_config.name
  source = var.query_config_file

  lifecycle {
    ignore_changes = [source, detect_md5hash]
  }
}
