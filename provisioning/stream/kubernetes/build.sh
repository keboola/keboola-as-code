#!/usr/bin/env bash
set -Eeuo pipefail

# CD to the script directory
cd "$(dirname "$0")"

rm -fR deploy
mkdir deploy

# Namespace
envsubst < templates/namespace.yaml > deploy/namespace.yaml

# Etcd
mkdir deploy/etcd
cp ../../common/etcd/values.yaml deploy/etcd/values_common.yaml
cp templates/etcd.yaml deploy/etcd/values_stream.yaml

# Config
mkdir deploy/config
envsubst < templates/config/config-map.yaml       > deploy/config/config-map.yaml

# API
mkdir deploy/api
envsubst < templates/api/pdb.yaml                 > deploy/api/pdb.yaml
envsubst < templates/api/network-policy.yaml      > deploy/api/network-policy.yaml
envsubst < templates/api/deployment.yaml          > deploy/api/deployment.yaml

# HTTP source
mkdir deploy/http-source
envsubst < templates/http-source/pdb.yaml                 > deploy/http-source/pdb.yaml
envsubst < templates/http-source/network-policy.yaml      > deploy/http-source/network-policy.yaml
envsubst < templates/http-source/deployment.yaml          > deploy/http-source/deployment.yaml

# Storage writer/reader
mkdir deploy/storage-writer-reader
envsubst < templates/storage-writer-reader/network-policy.yaml  > deploy/storage-writer-reader/network-policy.yaml
envsubst < templates/storage-writer-reader/statefulset.yaml     > deploy/storage-writer-reader/statefulset.yaml
envsubst < templates/storage-writer-reader/service.yaml         > deploy/storage-writer-reader/service.yaml

# Storage coordinator
mkdir deploy/storage-coordinator
envsubst < templates/storage-coordinator/pdb.yaml                 > deploy/storage-coordinator/pdb.yaml
envsubst < templates/storage-coordinator/network-policy.yaml      > deploy/storage-coordinator/network-policy.yaml
envsubst < templates/storage-coordinator/deployment.yaml          > deploy/storage-coordinator/deployment.yaml

# Remove resources requests/limits to fit all pods to the CI environment
REMOVE_RESOURCES_LIMITS="${REMOVE_RESOURCES_LIMITS:=false}"
if [[ "$REMOVE_RESOURCES_LIMITS" == "true" ]]; then
  echo
  echo "Removing resources requests/limits ..."
  echo "--------------------------"
  # In the regexp is backreference "\1", this ensures that only nested keys that follow are deleted
  # Storage resources - disk size, must be keep.
  find ./deploy -type f \( -iname \*.yml -o -iname \*.yaml \) \
  -exec echo "{}" \; \
  -exec perl -i.original -0777pe "s/(\n *)resources:\s*(?!(\1 +.+)+\1 +storage:)(\1 +.+)+/\1# <<<REMOVED RESOURCES KEY>>>/g" "{}" \;
fi

# Cloud specific
mkdir deploy/cloud
if [[ "$CLOUD_PROVIDER" == "aws" ]]; then
  mkdir deploy/cloud/aws
  envsubst < templates/cloud/aws/service-api.yaml > deploy/cloud/aws/service-api.yaml
  envsubst < templates/cloud/aws/ingress-api.yaml > deploy/cloud/aws/ingress-api.yaml
    envsubst < templates/cloud/aws/service-http-source.yaml > deploy/cloud/aws/service-http-source.yaml
    envsubst < templates/cloud/aws/ingress-http-source.yaml > deploy/cloud/aws/ingress-http-source.yaml
  envsubst < templates/cloud/aws/sc-stream-api.yaml > deploy/cloud/aws/sc-stream-api.yaml
elif [[ "$CLOUD_PROVIDER" == "gcp" ]]; then
  mkdir deploy/cloud/gcp
  envsubst < templates/cloud/gcp/service-api.yaml > deploy/cloud/gcp/service-api.yaml
  envsubst < templates/cloud/gcp/ingress-api.yaml > deploy/cloud/gcp/ingress-api.yaml
    envsubst < templates/cloud/gcp/service-http-source.yaml > deploy/cloud/gcp/service-http-source.yaml
    envsubst < templates/cloud/gcp/ingress-http-source.yaml > deploy/cloud/gcp/ingress-http-source.yaml
elif [[ "$CLOUD_PROVIDER" == "azure" ]]; then
  mkdir deploy/cloud/azure
  envsubst < templates/cloud/azure/service-api.yaml > deploy/cloud/azure/service-api.yaml
   envsubst < templates/cloud/azure/service-http-source.yaml > deploy/cloud/azure/service-http-source.yaml
   envsubst < templates/cloud/azure/sc-stream-api.yaml > deploy/cloud/azure/sc-stream-api.yaml
  envsubst < templates/cloud/azure/secret-infra.yaml > deploy/cloud/azure/secret-infra.yaml
  envsubst < templates/cloud/azure/serviceaccount.yaml > deploy/cloud/azure/serviceaccount.yaml
elif [[ "$CLOUD_PROVIDER" == "local" ]]; then
  mkdir deploy/cloud/local
  envsubst < templates/cloud/local/service-api.yaml > deploy/cloud/local/service-api.yaml
  envsubst < templates/cloud/local/service-http-source.yaml > deploy/cloud/local/service-http-source.yaml
  envsubst < templates/cloud/local/secret-infra.yaml > deploy/cloud/local/secret-infra.yaml
  envsubst < templates/cloud/local/serviceaccount.yaml > deploy/cloud/local/serviceaccount.yaml
fi
