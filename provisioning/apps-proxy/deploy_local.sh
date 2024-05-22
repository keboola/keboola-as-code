#!/usr/bin/env bash
set -Eeuo pipefail

# CD to the script directory
cd "$(dirname "$0")"

# Setup local environment
export CLOUD_PROVIDER=local

# Default values for the local deployment
export MINIKUBE_PROFILE="${MINIKUBE_PROFILE:=apps-proxy}"
export BUILD_BUILDID="${BUILD_BUILDID:=dev}"
export RELEASE_RELEASENAME="${RELEASE_RELEASENAME:=my-release}"
export KEBOOLA_STACK="${KEBOOLA_STACK:=local-machine}"
export HOSTNAME_SUFFIX="${HOSTNAME_SUFFIX:=keboola.com}"
export APPS_PROXY_REPOSITORY="${APPS_PROXY_REPOSITORY:=docker.io/keboola/apps-proxy}" # docker.io prefix is required
export APPS_PROXY_IMAGE_TAG="${APPS_PROXY_IMAGE_TAG:=$(git rev-parse --short HEAD)-$(date +%s)}"
export APPS_PROXY_REPLICAS="${APPS_PROXY_REPLICAS:=3}"
export APPS_PROXY_SANDBOXES_API_TOKEN="${APPS_PROXY_SANDBOXES_API_TOKEN:=token}"

./deploy.sh
