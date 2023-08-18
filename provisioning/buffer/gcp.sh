#!/usr/bin/env bash
set -Eeuo pipefail

terraform_output () {
  terraform -chdir=./gcp output -raw $1
}

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_PATH}"


if [ -n "${TF_INIT_ONLY:-}" ]; then
  terraform init -no-color -backend=false
  exit 0
fi

echo ""
echo "Terraform backend configuration:"
echo "bucket=${TERRAFORM_REMOTE_STATE_BUCKET}"
echo ""

terraform -chdir=./gcp  init -input=false -no-color \
  -backend-config="bucket=${TERRAFORM_REMOTE_STATE_BUCKET}" \
  -backend-config="prefix=keboola-as-code/buffer"

echo "=> Validating configuration"
terraform validate -no-color

echo "=> Planning changes"
terraform -chdir=./gcp plan -input=false -no-color  -out=terraform.tfplan \
  -var "terraform_remote_state_bucket=${TERRAFORM_REMOTE_STATE_BUCKET}"

echo "=> Applying changes"
terraform -chdir=./gcp apply -no-color terraform.tfplan

# Authorize to GKE
GKE_CLUSTER_NAME=$(terraform_output main_gke_cluster_name)
GKE_CLUSTER_LOCATION=$(terraform_output main_gke_cluster_location)

echo $GKE_CLUSTER_NAME
echo $GKE_CLUSTER_LOCATION

gcloud auth login --cred-file=$GOOGLE_APPLICATION_CREDENTIALS
gcloud container clusters get-credentials $GKE_CLUSTER_NAME --region $GKE_CLUSTER_LOCATION --project $GCP_PROJECT
