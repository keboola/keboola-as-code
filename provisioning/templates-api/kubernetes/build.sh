#!/usr/bin/env bash
set -Eeuo pipefail

# CD to the script directory
cd "$(dirname "$0")"

# Etcd cluster for Templates API
: "${TEMPLATES_API_ETCD_REPLICAS:=1}"
ETCD_ROOT_PASSWORD_BASE64=$(kubectl get secret --namespace templates-api templates-api-etcd -o jsonpath="{.data.etcd-root-password}" 2>/dev/null || echo -e '')
if [[ "$ETCD_ROOT_PASSWORD_BASE64" == "" ]]; then
  # Generate random root password if it not set
  ETCD_ROOT_PASSWORD_BASE64=$(LC_CTYPE=C tr -dc A-Za-z0-9 </dev/urandom | head -c 17 | base64 || echo -e '')
fi
# Generate list of cluster endpoints
ETCD_INITIAL_CLUSTER=$(seq 0 $(($TEMPLATES_API_ETCD_REPLICAS-1)) | awk '{ print "templates-api-etcd-"$0"=http://templates-api-etcd-"$0".templates-api-etcd-headless.templates-api.svc.cluster.local:2380"}' | sed -e 'H;${x;s/\n/,/g;s/^,//;p;};d')
export TEMPLATES_API_ETCD_REPLICAS
export ETCD_ROOT_PASSWORD_BASE64
export ETCD_INITIAL_CLUSTER
envsubst < templates/etcd.yaml > deploy/etcd.yaml

# Templates API
envsubst < templates/namespace.yaml > deploy/namespace.yaml
envsubst < templates/config-map.yaml > deploy/config-map.yaml
envsubst < templates/templates-api.yaml > deploy/templates-api.yaml

if [[ "$CLOUD_PROVIDER" == "aws" ]]; then
  envsubst < templates/aws/service.yaml > deploy/aws/service.yaml
  envsubst < templates/aws/ingress.yaml > deploy/aws/ingress.yaml
elif [[ "$CLOUD_PROVIDER" == "azure" ]]; then
  envsubst < templates/azure/service.yaml > deploy/azure/service.yaml
fi
