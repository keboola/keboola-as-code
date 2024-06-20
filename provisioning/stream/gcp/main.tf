terraform {
  required_version = "~> 1.1"
  backend "gcs" {}
}

# Read shared infrastructure outputs
data "terraform_remote_state" "infrastructure" {
  backend = "gcs"

  config = {
    bucket = var.terraform_remote_state_bucket
    prefix = "infrastructure/output"
  }
}

output "main_gke_cluster_name" {
  value = data.terraform_remote_state.infrastructure.outputs.main_gke_cluster_name
}

output "main_gke_cluster_location" {
  value = data.terraform_remote_state.infrastructure.outputs.main_gke_cluster_location
}