#!/usr/bin/env bash
set -Eeuo pipefail

# CD to the script directory
cd "$(dirname "$0")"

# Setup local environment
export CLOUD_PROVIDER=local

# Default values for the local deployment
export MINIKUBE_PROFILE="${MINIKUBE_PROFILE:=stream}"
export BUILD_BUILDID="${BUILD_BUILDID:=dev}"
export RELEASE_RELEASENAME="${RELEASE_RELEASENAME:=my-release}"
export KEBOOLA_STACK="${KEBOOLA_STACK:=local-machine}"
export HOSTNAME_SUFFIX="${HOSTNAME_SUFFIX:=keboola.com}"
export STREAM_IMAGE_REPOSITORY="${STREAM_IMAGE_REPOSITORY:=docker.io/keboola/stream-api}"
export STREAM_IMAGE_TAG="${STREAM_IMAGE_TAG:=$(git rev-parse --short HEAD)-$(date +%s)}"
export STREAM_API_REPLICAS="${STREAM_API_REPLICAS:=2}"
export STREAM_STORAGE_REPLICAS="${STREAM_STORAGE_REPLICAS:=2}"
export STREAM_COORDINATOR_REPLICAS="${STREAM_COORDINATOR_REPLICAS:=2}"
export STREAM_ETCD_REPLICAS="${STREAM_ETCD_REPLICAS:=3}"
export STREAM_ETCD_MEMORY="${STREAM_ETCD_MEMORY:="512Mi"}"

./deploy.sh
