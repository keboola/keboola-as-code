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
: ${STREAM_IMAGE_REPOSITORY?"Missing STREAM_IMAGE_REPOSITORY"}
: ${STREAM_IMAGE_TAG?"Missing STREAM_IMAGE_TAG"}
: #${K6_IMAGE_REPOSITORY?"Missing K6_IMAGE_REPOSITORY"}
: #${K6_IMAGE_TAG?"Missing K6_IMAGE_TAG"}
: ${STREAM_ETCD_REPLICAS?"Missing STREAM_ETCD_REPLICAS"}
: ${STREAM_API_REPLICAS?"Missing STREAM_API_REPLICAS"}
: ${STREAM_HTTP_SOURCE_REPLICAS?"Missing STREAM_HTTP_SOURCE_REPLICAS"}
: ${STREAM_STORAGE_READER_WRITER_REPLICAS?"Missing STREAM_STORAGE_READER_WRITER_REPLICAS"}
: ${STREAM_STORAGE_COORDINATOR_REPLICAS?"Missing STREAM_STORAGE_COORDINATOR_REPLICAS"}

: ${STREAM_ETCD_STORAGE_CLASS_NAME?"Missing ETCD_STORAGE_CLASS_NAME"}
: ${STREAM_VOLUME_FAST_STORAGE_CLASS_NAME?"Missing STREAM_VOLUME_FAST_STORAGE_CLASS_NAME"}
: ${STREAM_VOLUME_MEDIUM_STORAGE_CLASS_NAME?"Missing STREAM_VOLUME_MEDIUM_STORAGE_CLASS_NAME"}
: ${STREAM_VOLUME_SLOW_STORAGE_CLASS_NAME?"Missing STREAM_VOLUME_SLOW_STORAGE_CLASS_NAME"}

: ${STREAM_VOLUME_FAST_STORAGE_SIZE?"Missing STREAM_VOLUME_FAST_STORAGE_SIZE"}
: ${STREAM_VOLUME_MEDIUM_STORAGE_SIZE?"Missing STREAM_VOLUME_MEDIUM_STORAGE_SIZE"}
: ${STREAM_VOLUME_SLOW_STORAGE_SIZE?"Missing STREAM_VOLUME_SLOW_STORAGE_SIZE"}

# Default values

# Etcd memory usage depends primarily on the amount of stored data and ETCD_SNAPSHOT_COUNT setting.
# GOMEMLIMIT env is set, so etcd garbage collector knows what the maximum is and will not accidentally exceed it.
export STREAM_ETCD_MEMORY_SOFT_LIMIT="${STREAM_ETCD_MEMORY:="1800Mi"}"
export STREAM_ETCD_MEMORY_HARD_LIMIT="${STREAM_ETCD_MEMORY:="2000Mi"}"

# Configuration API reads and writes stream configurations to etcd, waits for resources to be created in the Storage API.
# Memory/CPU usage is minimal.
export STREAM_API_MEMORY_SOFT_LIMIT="${STREAM_API_MEMORY_SOFT_LIMIT:="250Mi"}"
export STREAM_API_MEMORY_HARD_LIMIT="${STREAM_API_MEMORY_HARD_LIMIT:="300Mi"}"

# HTTP source receives records by a HTTP server, converts records to CSV and compresses them.
# Memory/CPU usage increases with requests rate.
export STREAM_HTTP_SOURCE_MEMORY_SOFT_LIMIT="${STREAM_HTTP_SOURCE_MEMORY_SOFT_LIMIT:="900Mi"}"
export STREAM_HTTP_SOURCE_MEMORY_HARD_LIMIT="${STREAM_HTTP_SOURCE_MEMORY_HARD_LIMIT:="1000Mi"}"

# Storage writer receives compressed CSV bytes and stores them to the local disk.
# Sufficiently memory size and disks speed are required.
export STREAM_STORAGE_WRITER_MEMORY_SOFT_LIMIT="${STREAM_STORAGE_WRITER_MEMORY_SOFT_LIMIT:="900Mi"}"
export STREAM_STORAGE_WRITER_MEMORY_HARD_LIMIT="${STREAM_STORAGE_WRITER_MEMORY_HARD_LIMIT:="1000Mi"}"

# Storage reader uploads slices from the local disk to a staging storage.
# Sufficiently disks speed is required.
export STREAM_STORAGE_READER_MEMORY_SOFT_LIMIT="${STREAM_STORAGE_READER_MEMORY_SOFT_LIMIT:="450Mi"}"
export STREAM_STORAGE_READER_MEMORY_HARD_LIMIT="${STREAM_STORAGE_READER_MEMORY_HARD_LIMIT:="500Mi"}"

# Storage coordinator watches statistics and based on them, triggers slice upload and file import by modifying the state of the entity in the etcd.
# Memory/CPU usage is minimal.
export STREAM_STORAGE_COORDINATOR_MEMORY_SOFT_LIMIT="${STREAM_STORAGE_COORDINATOR_MEMORY_SOFT_LIMIT:="250Mi"}"
export STREAM_STORAGE_COORDINATOR_MEMORY_HARD_LIMIT="${STREAM_STORAGE_COORDINATOR_MEMORY_HARD_LIMIT:="300Mi"}"

# Constants
export NAMESPACE="stream"
ETCD_HELM_CHART_VERSION="10.2.4"

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
  --set "persistence.storageClass=$STREAM_ETCD_STORAGE_CLASS_NAME" \
  --set "resources.requests.memory=$STREAM_ETCD_MEMORY_SOFT_LIMIT" \
  --set "resources.limits.memory=$STREAM_ETCD_MEMORY_HARD_LIMIT" \
  --set "extraEnvVars[3].name=GOMEMLIMIT" \
  --set "extraEnvVars[3].value=${STREAM_ETCD_MEMORY_SOFT_LIMIT}B"

# Config
kubectl apply -f ./kubernetes/deploy/config/config-map.yaml

# API
kubectl apply -f ./kubernetes/deploy/api/pdb.yaml
kubectl apply -f ./kubernetes/deploy/api/network-policy.yaml
kubectl apply -f ./kubernetes/deploy/api/deployment.yaml

# HTTP source
kubectl apply -f ./kubernetes/deploy/http-source/pdb.yaml
kubectl apply -f ./kubernetes/deploy/http-source/network-policy.yaml
kubectl apply -f ./kubernetes/deploy/http-source/deployment.yaml

# Storage writer/reader
kubectl apply -f ./kubernetes/deploy/storage-writer-reader/network-policy.yaml
kubectl apply -f ./kubernetes/deploy/storage-writer-reader/statefulset.yaml
kubectl apply -f ./kubernetes/deploy/storage-writer-reader/service.yaml

# Storage coordinator
kubectl apply -f ./kubernetes/deploy/storage-coordinator/pdb.yaml
kubectl apply -f ./kubernetes/deploy/storage-coordinator/network-policy.yaml
kubectl apply -f ./kubernetes/deploy/storage-coordinator/deployment.yaml
