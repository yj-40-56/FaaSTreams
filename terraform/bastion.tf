# Memorystore has no public IP, so Redis is reached from a laptop through this
# VM. The external IP is for outbound package installs (redis-tools).

resource "google_compute_firewall" "allow_iap_ssh" {
  count = var.enable_redis_bastion ? 1 : 0

  name    = "${var.network_name}-allow-iap-ssh"
  network = google_compute_network.vpc.id

  direction     = "INGRESS"
  source_ranges = ["35.235.240.0/20"] # Google's IAP forwarding range
  target_tags   = ["redis-bastion"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_firewall" "allow_ssh_external" {
  count = var.enable_redis_bastion && var.bastion_allow_external_ssh ? 1 : 0

  name    = "${var.network_name}-allow-ssh-external"
  network = google_compute_network.vpc.id

  direction     = "INGRESS"
  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["redis-bastion"]

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

resource "google_compute_instance" "redis_bastion" {
  count = var.enable_redis_bastion ? 1 : 0

  name         = "faastreams-redis-bastion"
  machine_type = "e2-micro"
  zone         = "${var.region}-a"
  tags         = ["redis-bastion"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
    }
  }

  network_interface {
    network    = google_compute_network.vpc.id
    subnetwork = google_compute_subnetwork.subnet.id
    access_config {}
  }

  # redis-tools on first boot, so --clean can FLUSHALL without a manual install.
  metadata = {
    enable-oslogin = "TRUE"
    startup-script = "command -v redis-cli >/dev/null || (apt-get update && apt-get install -y redis-tools)"
  }
}
