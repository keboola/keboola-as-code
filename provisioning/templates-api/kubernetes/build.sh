#!/usr/bin/env bash
set -Eeuo pipefail

# CD to the script directory
cd "$(dirname "$0")"

# Etcd
envsubst < templates/etcd-values.yaml > deploy/etcd-values.yaml

# Templates API
envsubst < templates/namespace.yaml > deploy/namespace.yaml
envsubst < templates/config-map.yaml > deploy/config-map.yaml
envsubst < templates/templates-api.yaml > deploy/templates-api.yaml

if [[ "$CLOUD_PROVIDER" == "aws" ]]; then
  envsubst < templates/aws/service.yaml > deploy/aws/service.yaml
  envsubst < templates/aws/ingress.yaml > deploy/aws/ingress.yaml
elif [[ "$CLOUD_PROVIDER" == "azure" ]]; then
  envsubst < templates/azure/service.yaml > deploy/azure/service.yaml
elif [[ "$CLOUD_PROVIDER" == "local" ]]; then
  envsubst < templates/local/service.yaml > deploy/local/service.yaml
fi
