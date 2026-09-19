terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source = "hashicorp/google"
      # 7.10 added direct VPC egress to google_cloudfunctions2_function.
      version = "~> 8.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }

  # Partial config: the bucket lives in the target project, so it comes from
  # environments/<project>.backend.hcl at init time.
  backend "gcs" {}
}

provider "google" {
  project = var.project_id
  region  = var.region
}
