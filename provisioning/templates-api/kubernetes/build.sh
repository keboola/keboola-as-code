#!/usr/bin/env bash
set -Eeuo pipefail

# CD to the script directory
cd "$(dirname "$0")"

# Namespace
envsubst < templates/namespace.yaml > deploy/namespace.yaml

# Etcd
cp ../../common/etcd/values.yaml deploy/etcd/values.yaml

# API
envsubst < templates/api/config-map.yaml > deploy/api/config-map.yaml
envsubst < templates/api/pdb.yaml        > deploy/api/pdb.yaml
envsubst < templates/api/deployment.yaml > deploy/api/deployment.yaml

# Cloud specific
if [[ "$CLOUD_PROVIDER" == "aws" ]]; then
  envsubst < templates/cloud/aws/service.yaml > deploy/cloud/aws/service.yaml
  envsubst < templates/cloud/aws/ingress.yaml > deploy/cloud/aws/ingress.yaml
elif [[ "$CLOUD_PROVIDER" == "azure" ]]; then
  envsubst < templates/cloud/azure/service.yaml > deploy/cloud/azure/service.yaml
elif [[ "$CLOUD_PROVIDER" == "local" ]]; then
  envsubst < templates/cloud/local/service.yaml > deploy/cloud/local/service.yaml
fi
