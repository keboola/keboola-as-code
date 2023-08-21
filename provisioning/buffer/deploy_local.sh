#!/usr/bin/env bash
set -Eeuo pipefail

# CD to the script directory
cd "$(dirname "$0")"

# Setup local environment
export CLOUD_PROVIDER=local

# Default values for the local deployment
export MINIKUBE_PROFILE="${MINIKUBE_PROFILE:=buffer}"
export BUILD_BUILDID="${BUILD_BUILDID:=dev}"
export RELEASE_RELEASENAME="${RELEASE_RELEASENAME:=my-release}"
export KEBOOLA_STACK="${KEBOOLA_STACK:=local-machine}"
export HOSTNAME_SUFFIX="${HOSTNAME_SUFFIX:=keboola.com}"
export BUFFER_API_REPOSITORY="${BUFFER_API_REPOSITORY:=docker.io/keboola/buffer-api}"
export BUFFER_API_IMAGE_TAG="${BUFFER_API_IMAGE_TAG:=$(git rev-parse --short HEAD)}"
export BUFFER_WORKER_REPOSITORY="${BUFFER_WORKER_REPOSITORY:=docker.io/keboola/buffer-worker}"
export BUFFER_WORKER_IMAGE_TAG="${BUFFER_WORKER_IMAGE_TAG:=$(git rev-parse --short HEAD)}"
export BUFFER_API_REPLICAS="${BUFFER_API_REPLICAS:=2}"
export BUFFER_WORKER_REPLICAS="${BUFFER_WORKER_REPLICAS:=2}"
export BUFFER_ETCD_REPLICAS="${BUFFER_ETCD_REPLICAS:=3}"
export BUFFER_ETCD_MEMORY="${BUFFER_ETCD_MEMORY:="512Mi"}"

./deploy.sh
