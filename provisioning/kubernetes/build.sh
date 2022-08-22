#!/usr/bin/env bash
set -Eeuo pipefail

# Etcd cluster for Templates API
: "${ETCD_REPLICA_COUNT:=3}"
ETCD_ROOT_PASSWORD_BASE64=$(kubectl get secret --namespace default templates-api-etcd -o jsonpath="{.data.etcd-root-password}" 2>/dev/null || echo -e '')
if [[ "$ETCD_ROOT_PASSWORD_BASE64" == "" ]]; then
  # Generate random root password if it not set
  ETCD_ROOT_PASSWORD_BASE64=$(tr -dc A-Za-z0-9 </dev/urandom | head -c 17 | base64 || echo -e '')
fi
# Generate list of cluster endpoints
ETCD_INITIAL_CLUSTER=$(seq 0 $(($ETCD_REPLICA_COUNT-1)) | awk '{ print "templates-api-etcd-"$0"=http://templates-api-etcd-"$0".templates-api-etcd-headless.default.svc.cluster.local:2380"}' | paste -d',' -s)
export ETCD_REPLICA_COUNT
export ETCD_ROOT_PASSWORD_BASE64
export ETCD_INITIAL_CLUSTER
envsubst < provisioning/kubernetes/templates/etcd.yaml > provisioning/kubernetes/deploy/etcd.yaml

# Templates API
envsubst < provisioning/kubernetes/templates/config-map.yaml > provisioning/kubernetes/deploy/config-map.yaml
envsubst < provisioning/kubernetes/templates/templates-api.yaml > provisioning/kubernetes/deploy/templates-api.yaml
envsubst < provisioning/kubernetes/templates/"$CLOUD_PROVIDER"/service.yaml > provisioning/kubernetes/deploy/"$CLOUD_PROVIDER"/service.yaml

if [[ "$CLOUD_PROVIDER" == "aws" ]]; then
  envsubst < provisioning/kubernetes/templates/aws/ingress.yaml > provisioning/kubernetes/deploy/aws/ingress.yaml
fi
