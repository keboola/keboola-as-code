#!/usr/bin/env bash
set -Eeuo pipefail

# CD to the script directory
cd "$(dirname "$0")"

# Namespace
envsubst < templates/namespace.yaml > deploy/namespace.yaml

# Etcd
cp ../../common/etcd/values.yaml deploy/etcd/values_common.yaml
cp templates/etcd.yaml deploy/etcd/values_stream.yaml

# Config
envsubst < templates/config/config-map.yaml       > deploy/config/config-map.yaml

# API
envsubst < templates/api/pdb.yaml                 > deploy/api/pdb.yaml
envsubst < templates/api/network-policy.yaml      > deploy/api/network-policy.yaml
envsubst < templates/api/deployment.yaml          > deploy/api/deployment.yaml

# Storage
envsubst < templates/storage/network-policy.yaml  > deploy/storage/network-policy.yaml
envsubst < templates/storage/statefulset.yaml     > deploy/storage/statefulset.yaml

# Remove resources requests/limits to fit all pods to the CI environment
REMOVE_RESOURCES_LIMITS="${REMOVE_RESOURCES_LIMITS:=false}"
if [[ "$REMOVE_RESOURCES_LIMITS" == "true" ]]; then
  echo
  echo "Removing resources requests/limits ..."
  echo "--------------------------"
  # In the regexp is backreference "\1", this ensures that only nested keys that follow are deleted
  find ./deploy -type f \( -iname \*.yml -o -iname \*.yaml \) \
  -exec echo "{}" \; \
  -exec perl -i.original -0777pe "s/(\n *)resources:\s*(\1 +.+)+/\1# <<<REMOVED RESOURCES KEY>>>/g" "{}" \;
fi

# Cloud specific
if [[ "$CLOUD_PROVIDER" == "aws" ]]; then
  envsubst < templates/cloud/aws/service.yaml > deploy/cloud/aws/service.yaml
  envsubst < templates/cloud/aws/ingress.yaml > deploy/cloud/aws/ingress.yaml
elif [[ "$CLOUD_PROVIDER" == "gcp" ]]; then
  envsubst < templates/cloud/gcp/service.yaml > deploy/cloud/gcp/service.yaml
  envsubst < templates/cloud/gcp/ingress.yaml > deploy/cloud/gcp/ingress.yaml
elif [[ "$CLOUD_PROVIDER" == "azure" ]]; then
  envsubst < templates/cloud/azure/service.yaml > deploy/cloud/azure/service.yaml
elif [[ "$CLOUD_PROVIDER" == "local" ]]; then
  envsubst < templates/cloud/local/service.yaml > deploy/cloud/local/service.yaml
fi
