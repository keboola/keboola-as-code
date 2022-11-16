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
export BUFFER_ETCD_REPLICAS="${BUFFER_ETCD_REPLICAS:=3}"

./deploy.sh
