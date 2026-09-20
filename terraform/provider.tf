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

  # Partial config: the bucket lives in the target project and a backend block
  # can't read variables, so `make init` passes it as <project>-terraform-state.
  backend "gcs" {}
}

provider "google" {
  project = var.project_id
  region  = var.region
}
