#!/usr/bin/env bash
set -Eeuo pipefail

# CD to the script directory
cd "$(dirname "$0")"

# Namespace
envsubst < templates/namespace.yaml > deploy/namespace.yaml

# Proxy
envsubst < templates/proxy/config-map.yaml     > deploy/proxy/config-map.yaml
envsubst < templates/proxy/token.yaml          > deploy/proxy/token.yaml
envsubst < templates/proxy/pdb.yaml            > deploy/proxy/pdb.yaml
envsubst < templates/proxy/network-policy.yaml > deploy/proxy/network-policy.yaml
envsubst < templates/proxy/deployment.yaml     > deploy/proxy/deployment.yaml

# CSAS migration, without condition because it's trashed after build
envsubst < templates/nginx-redir/deployment.yaml > deploy/nginx-redir/deployment.yaml
# required NGINX configuration breaks ideal templating so skipping it
cat templates/nginx-redir/config-map.yaml        > deploy/nginx-redir/config-map.yaml

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
elif [[ "$CLOUD_PROVIDER" == "azure" ]]; then
  envsubst < templates/cloud/azure/service.yaml > deploy/cloud/azure/service.yaml
elif [[ "$CLOUD_PROVIDER" == "gcp" ]]; then
  envsubst < templates/cloud/gcp/service.yaml > deploy/cloud/gcp/service.yaml
  envsubst < templates/cloud/gcp/ingress.yaml > deploy/cloud/gcp/ingress.yaml
elif [[ "$CLOUD_PROVIDER" == "local" ]]; then
  envsubst < templates/cloud/local/service.yaml > deploy/cloud/local/service.yaml
fi
