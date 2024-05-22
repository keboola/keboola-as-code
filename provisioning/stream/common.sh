#!/usr/bin/env bash

## Prevent direct run of the script
#if [ "${BASH_SOURCE[0]}" -ef "$0" ]
#then
#    echo 'This script should not be executed directly, please run "deploy.sh" instead.'
#    exit 1
#fi

# Required ENVs
: ${RELEASE_ID?"Missing RELEASE_ID"}
: ${KEBOOLA_STACK?"Missing KEBOOLA_STACK"}
: ${HOSTNAME_SUFFIX?"Missing HOSTNAME_SUFFIX"}
: ${STREAM_IMAGE_REPOSITORY?"Missing STREAM_IMAGE_REPOSITORY"}
: ${STREAM_IMAGE_TAG?"Missing STREAM_IMAGE_TAG"}
: ${STREAM_API_REPLICAS?"Missing STREAM_API_REPLICAS"}
: ${STREAM_STORAGE_REPLICAS?"Missing STREAM_SOURCE_REPLICAS"}
: ${STREAM_COORDINATOR_REPLICAS?"Missing STREAM_COORDINATOR_REPLICAS"}
: ${STREAM_ETCD_REPLICAS?"Missing STREAM_ETCD_REPLICAS"}
: ${ETCD_STORAGE_CLASS_NAME?"Missing ETCD_STORAGE_CLASS_NAME"}

# Default values
export STREAM_ETCD_MEMORY="${STREAM_ETCD_MEMORY:="2000Mi"}"

# Constants
export NAMESPACE="stream"
ETCD_HELM_CHART_VERSION="10.1.0"

# Common part of the deployment. Same for AWS/Azure/Local
./kubernetes/build.sh

# Namespace
kubectl apply -f ./kubernetes/deploy/namespace.yaml

# Get etcd root password, if it is already present
export ETCD_ROOT_PASSWORD=$(kubectl get secret --namespace "$NAMESPACE" stream-etcd -o jsonpath="{.data.etcd-root-password}" 2>/dev/null | base64 -d)

# Deploy etcd cluster
helm repo add --force-update bitnami https://charts.bitnami.com/bitnami
helm upgrade \
  --install stream-etcd bitnami/etcd \
  --version "$ETCD_HELM_CHART_VERSION" \
  --values ./kubernetes/deploy/etcd/values_common.yaml \
  --values ./kubernetes/deploy/etcd/values_stream.yaml \
  --namespace "$NAMESPACE" \
  --set "replicaCount=$STREAM_ETCD_REPLICAS" \
  --set "auth.rbac.rootPassword=$ETCD_ROOT_PASSWORD" \
  --set "persistence.storageClass=$ETCD_STORAGE_CLASS_NAME" \
  --set "resources.requests.memory=$STREAM_ETCD_MEMORY" \
  --set "resources.limits.memory=$STREAM_ETCD_MEMORY" \
  --set "extraEnvVars[3].name=GOMEMLIMIT" \
  --set "extraEnvVars[3].value=${STREAM_ETCD_MEMORY}B"

# Config
kubectl apply -f ./kubernetes/deploy/config/config-map.yaml

# API
kubectl apply -f ./kubernetes/deploy/api/pdb.yaml
kubectl apply -f ./kubernetes/deploy/api/network-policy.yaml
kubectl apply -f ./kubernetes/deploy/api/deployment.yaml

# Storage
kubectl apply -f ./kubernetes/deploy/storage/network-policy.yaml
kubectl apply -f ./kubernetes/deploy/storage/statefulset.yaml
