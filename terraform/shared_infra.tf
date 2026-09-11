# Shared infrastructure: the Memorystore Redis instance every service reads/writes,
# and the Serverless VPC Access connector that gives those services a route to it.
#
# Both were created by hand long before this Terraform config existed and are now
# managed here (previously they were referenced by IP/name only). Exactly ONE of
# each exists for the whole project, so neither is namespaced with `name_suffix`:
# a sandbox env shares live's Redis and connector rather than cloning them. That
# was already true in practice — every env pointed at the same hardcoded IP — and
# cloning a Memorystore instance per sandbox would bill real money to isolate
# something that Redis key prefixes already isolate.
#
# Because they're shared, only the `live` workspace declares them as resources.
# Other workspaces read the same objects through data sources, so a sandbox apply
# can't fight live over ownership (or fail on "already exists").
#
# *** These are stateful and shared. Import them; never create them fresh. ***
# `make import-live` (terraform/Makefile) covers both. See terraform/README.md.

locals {
  # Only the live workspace owns the shared objects; every other env reads them.
  manage_shared_infra = var.env_name == "live"
}

resource "google_redis_instance" "this" {
  count = local.manage_shared_infra ? 1 : 0

  name           = var.redis_instance_name
  project        = var.project_id
  region         = var.region
  tier           = var.redis_tier
  memory_size_gb = var.redis_memory_size_gb
  redis_version  = var.redis_version

  authorized_network = data.google_compute_network.default.id

  lifecycle {
    # Destroying this wipes every window/result the pipeline has accumulated and
    # takes the whole pipeline down — it is never a side effect of another change.
    # Removing the instance is a deliberate `terraform state rm` + manual delete.
    prevent_destroy = true

    # Set at creation time by hand and unknown to this config (the project's
    # billing is disabled as of 2026-09-11, so the live values could not be read
    # back to confirm them). Changing either forces replacement of the instance,
    # so defer to whatever is live instead of risking a destroy/recreate on a
    # value this config only guessed at. Drop these once the real values are
    # confirmed and written into the defaults below.
    ignore_changes = [
      reserved_ip_range,
      redis_version,
    ]
  }
}

data "google_redis_instance" "shared" {
  count = local.manage_shared_infra ? 0 : 1

  name    = var.redis_instance_name
  project = var.project_id
  region  = var.region
}

# Live config read from the API on 2026-09-11 (this one is still queryable without
# billing): network "default", 10.8.0.0/28, e2-micro, 2-3 instances. Throughput is
# deliberately not set — min_throughput/max_throughput conflict with
# min_instances/max_instances in the provider, and the live 200/300 is exactly what
# 2-3 e2-micro instances yield.
resource "google_vpc_access_connector" "this" {
  count = local.manage_shared_infra ? 1 : 0

  name    = var.vpc_connector_name
  project = var.project_id
  region  = var.region

  network       = data.google_compute_network.default.name
  ip_cidr_range = var.vpc_connector_cidr
  machine_type  = var.vpc_connector_machine_type
  min_instances = var.vpc_connector_min_instances
  max_instances = var.vpc_connector_max_instances

  lifecycle {
    # Every managed function attaches to this connector; destroying it severs all
    # of them from Redis at once.
    prevent_destroy = true
  }
}

data "google_vpc_access_connector" "shared" {
  count = local.manage_shared_infra ? 0 : 1

  name    = var.vpc_connector_name
  project = var.project_id
  region  = var.region
}

# The auto-mode VPC that Redis and the connector both sit on. Read-only on purpose:
# it is the project's default network, shared with resources this repo knows nothing
# about (the redis-bastion VM, the hand-deployed push ingestor), so Terraform reads
# it rather than claiming ownership of it.
data "google_compute_network" "default" {
  name    = var.network_name
  project = var.project_id
}
