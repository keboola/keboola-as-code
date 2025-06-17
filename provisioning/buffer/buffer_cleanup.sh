#!/usr/bin/env bash
set -Eeuo pipefail

# =========================
# Buffer Namespace Cleanup
# =========================
# This script authorizes to the Kubernetes cluster (AWS or Azure)
# a) lists all resources that will be deleted
# b) deletes the entire 'buffer' namespace and all PersistentVolumes bound to PVCs in this namespace
#
# Usage:
#   ./buffer_cleanup.sh [--dry-run]
#
# Required ENV:
#   CLOUD_PROVIDER=aws|azure
#   (AWS)   AWS_REGION, AWS_EKS_CLUSTER_NAME
#   (Azure) RESOURCE_GROUP
# =========================

DRY_RUN=0
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
  echo "[INFO] Running in DRY-RUN mode. No resources will be deleted."
fi

# --- Check required ENV ---
: ${CLOUD_PROVIDER?"Missing CLOUD_PROVIDER (aws|azure)"}

# --- Go to script directory ---
cd "$(dirname "$0")"

# --- Authorize to Kubernetes cluster ---
echo "[INFO] Authorizing to Kubernetes cluster for provider: $CLOUD_PROVIDER"
case "$CLOUD_PROVIDER" in
  aws)
    : ${AWS_REGION?"Missing AWS_REGION"}
    : ${AWS_EKS_CLUSTER_NAME?"Missing AWS_EKS_CLUSTER_NAME"}
    echo "[INFO] Using AWS EKS: $AWS_EKS_CLUSTER_NAME in $AWS_REGION"
    aws eks update-kubeconfig --name "$AWS_EKS_CLUSTER_NAME" --region "$AWS_REGION"
    ;;
  azure)
    : ${RESOURCE_GROUP?"Missing RESOURCE_GROUP"}
    echo "[INFO] Looking up AKS cluster name in resource group: $RESOURCE_GROUP"
    CLUSTER_NAME=$(az deployment group show \
      --resource-group "$RESOURCE_GROUP" \
      --name kbc-aks \
      --query "properties.outputs.clusterName.value" \
      --output tsv)
    echo "[INFO] Using Azure AKS: $CLUSTER_NAME"
    az aks get-credentials --name "$CLUSTER_NAME" --resource-group "$RESOURCE_GROUP" --overwrite-existing
    ;;
  *)
    echo "[ERROR] Unsupported CLOUD_PROVIDER: $CLOUD_PROVIDER" >&2
    exit 1
    ;;
esac

# --- List all resources in buffer namespace ---
echo "[INFO] Listing all resources in namespace 'buffer':"
kubectl api-resources --verbs=list --namespaced -o name | xargs -n 1 kubectl get --ignore-not-found -n buffer

# --- List all PersistentVolumes bound to buffer namespace ---
echo "[INFO] Listing PersistentVolumes bound to 'buffer' namespace:"
kubectl get pv -o json | jq -r '.items[] | select(.spec.claimRef.namespace=="buffer") | .metadata.name' || true

if [[ $DRY_RUN -eq 1 ]]; then
  echo "[DRY-RUN] No resources will be deleted. Exiting."
  exit 0
fi

# --- Delete namespace and PVs ---
echo "[INFO] Deleting namespace 'buffer' ..."
kubectl delete namespace buffer --ignore-not-found

echo "[INFO] Waiting for namespace 'buffer' to terminate ..."
kubectl wait --for=delete namespace/buffer --timeout=180s || true

# --- Delete all PersistentVolumes that were bound to PVCs in the 'buffer' namespace ---
echo "[INFO] Searching for PersistentVolumes bound to 'buffer' namespace ..."
PVS_TO_DELETE=$(kubectl get pv -o jsonpath='{range .items[?(@.spec.claimRef.namespace=="buffer")]}{.metadata.name}{"\n"}{end}')
if [[ -n "$PVS_TO_DELETE" ]]; then
  echo "[INFO] Deleting PersistentVolumes:"
  echo "$PVS_TO_DELETE"
  for pv in $PVS_TO_DELETE; do
    kubectl delete pv "$pv"
  done
else
  echo "[INFO] No PersistentVolumes found for 'buffer' namespace."
fi

# --- Final check: Ensure everything is deleted ---
ERROR=0

echo "[INFO] Checking for remaining resources ..."
REMAINING_PVS=$(kubectl get pv --no-headers | grep -w buffer || true)
if [[ -n "$REMAINING_PVS" ]]; then
  echo "[ERROR] Some PersistentVolumes still exist for 'buffer':"
  echo "$REMAINING_PVS"
  ERROR=1
else
  echo "[INFO] No PersistentVolumes for 'buffer' remain."
fi

if kubectl get namespace buffer &>/dev/null; then
  echo "[ERROR] Namespace 'buffer' still exists!"
  ERROR=1
else
  echo "[INFO] Namespace 'buffer' is deleted."
fi

if [[ $ERROR -eq 0 ]]; then
  echo "[SUCCESS] All resources for 'buffer' have been deleted."
else
  echo "[FAIL] Some resources for 'buffer' remain. Please check manually."
  exit 1
fi 