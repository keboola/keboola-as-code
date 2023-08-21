#!/usr/bin/env bash

# Prevent direct run of the script
if [ "${BASH_SOURCE[0]}" -ef "$0" ]
then
    echo 'This script should not be executed directly, please run "deploy.sh" instead.'
    exit 1
fi

# Required ENVs
: ${RELEASE_ID?"Missing RELEASE_ID"}
: ${KEBOOLA_STACK?"Missing KEBOOLA_STACK"}
: ${HOSTNAME_SUFFIX?"Missing HOSTNAME_SUFFIX"}
: ${BUFFER_API_REPOSITORY?"Missing BUFFER_API_REPOSITORY"}
: ${BUFFER_API_IMAGE_TAG?"Missing BUFFER_API_IMAGE_TAG"}
: ${BUFFER_WORKER_REPOSITORY?"Missing BUFFER_WORKER_REPOSITORY"}
: ${BUFFER_WORKER_IMAGE_TAG?"Missing BUFFER_WORKER_IMAGE_TAG"}
: ${BUFFER_API_REPLICAS?"Missing BUFFER_API_REPLICAS"}
: ${BUFFER_WORKER_REPLICAS?"Missing BUFFER_WORKER_REPLICAS"}
: ${BUFFER_ETCD_REPLICAS?"Missing BUFFER_ETCD_REPLICAS"}
: ${ETCD_STORAGE_CLASS_NAME?"Missing ETCD_STORAGE_CLASS_NAME"}

# Default values
export BUFFER_ETCD_MEMORY="${BUFFER_ETCD_MEMORY:="2000Mi"}"

# Constants
export NAMESPACE="buffer"
ETCD_HELM_CHART_VERSION="8.5.8"

# Common part of the deployment. Same for AWS/Azure/Local
./kubernetes/build.sh

# Namespace
kubectl apply -f ./kubernetes/deploy/namespace.yaml

# Get etcd root password, if it is already present
export ETCD_ROOT_PASSWORD=$(kubectl get secret --namespace "$NAMESPACE" buffer-etcd -o jsonpath="{.data.etcd-root-password}" 2>/dev/null | base64 -d)

# Deploy etcd cluster
helm repo add --force-update bitnami https://charts.bitnami.com/bitnami
helm upgrade \
  --install buffer-etcd bitnami/etcd \
  --version "$ETCD_HELM_CHART_VERSION" \
  --values ./kubernetes/deploy/etcd/values_common.yaml \
  --values ./kubernetes/deploy/etcd/values_buffer.yaml \
  --namespace "$NAMESPACE" \
  --set "replicaCount=$BUFFER_ETCD_REPLICAS" \
  --set "auth.rbac.rootPassword=$ETCD_ROOT_PASSWORD" \
  --set "persistence.storageClass=$ETCD_STORAGE_CLASS_NAME" \
  --set "resources.requests.memory=$BUFFER_ETCD_MEMORY" \
  --set "resources.limits.memory=$BUFFER_ETCD_MEMORY"

# API
kubectl apply -f ./kubernetes/deploy/api/config-map.yaml
kubectl apply -f ./kubernetes/deploy/api/pdb.yaml
kubectl apply -f ./kubernetes/deploy/api/network-policy.yaml
kubectl apply -f ./kubernetes/deploy/api/deployment.yaml

# Worker
kubectl apply -f ./kubernetes/deploy/worker/config-map.yaml
kubectl apply -f ./kubernetes/deploy/worker/pdb.yaml
kubectl apply -f ./kubernetes/deploy/worker/network-policy.yaml
kubectl apply -f ./kubernetes/deploy/worker/deployment.yaml
