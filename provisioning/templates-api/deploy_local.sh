#!/usr/bin/env bash
set -Eeuo pipefail

# CD to the script directory
cd "$(dirname "$0")"

# Setup local environment
export CLOUD_PROVIDER=local

# Default values for the local deployment
export MINIKUBE_PROFILE="${MINIKUBE_PROFILE:=templates-api}"
export BUILD_BUILDID="${BUILD_BUILDID:=dev}"
export RELEASE_RELEASENAME="${RELEASE_RELEASENAME:=my-release}"
export KEBOOLA_STACK="${KEBOOLA_STACK:=local-machine}"
export HOSTNAME_SUFFIX="${HOSTNAME_SUFFIX:=keboola.com}"
export TEMPLATES_API_REPOSITORY="${TEMPLATES_API_REPOSITORY:=docker.io/keboola/templates-api}" # docker.io prefix is required
export TEMPLATES_API_IMAGE_TAG="${TEMPLATES_API_IMAGE_TAG:=$(git rev-parse --short HEAD)-$(date +%s)}"
export TEMPLATES_API_REPLICAS="${TEMPLATES_API_REPLICAS:=3}"
export TEMPLATES_API_ETCD_REPLICAS="${TEMPLATES_API_ETCD_REPLICAS:=1}"

./deploy.sh
