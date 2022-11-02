#!/usr/bin/env bash
set -Eeuo pipefail

# CD to the script directory
cd "$(dirname "$0")"

# Etcd
envsubst < templates/etcd/values.yaml > deploy/etcd/values.yaml

# Templates API
envsubst < templates/namespace.yaml > deploy/namespace.yaml
envsubst < templates/config-map.yaml > deploy/config-map.yaml
envsubst < templates/templates-api.yaml > deploy/templates-api.yaml

if [[ "$CLOUD_PROVIDER" == "aws" ]]; then
  envsubst < templates/cloud/aws/service.yaml > deploy/cloud/aws/service.yaml
  envsubst < templates/cloud/aws/ingress.yaml > deploy/cloud/aws/ingress.yaml
elif [[ "$CLOUD_PROVIDER" == "azure" ]]; then
  envsubst < templates/cloud/azure/service.yaml > deploy/cloud/azure/service.yaml
elif [[ "$CLOUD_PROVIDER" == "local" ]]; then
  envsubst < templates/cloud/local/service.yaml > deploy/cloud/local/service.yaml
fi
