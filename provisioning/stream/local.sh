#!/usr/bin/env bash
set -Eeuo pipefail

# Prevent direct run of the script
if [ "${BASH_SOURCE[0]}" -ef "$0" ]
then
    echo 'This script should not be executed directly, please run "deploy_local.sh" instead.'
    exit 1
fi

# Default values for the local deployment
export MINIKUBE_PROFILE="${MINIKUBE_PROFILE:=stream}"
export KEBOOLA_STACK="${KEBOOLA_STACK:=local-machine}"
export HOSTNAME_SUFFIX="${HOSTNAME_SUFFIX:=keboola.com}"
export STREAM_IMAGE_REPOSITORY="${STREAM_IMAGE_REPOSITORY:=docker.io/keboola/stream-api}"
export STREAM_IMAGE_TAG="${STREAM_IMAGE_TAG:=$(git rev-parse --short HEAD)-$(date +%s)}"
export K6_IMAGE_REPOSITORY="${K6_IMAGE_REPOSITORY:=docker.io/keboolabot/stream-benchmark}"
export K6_IMAGE_TAG="${K6_IMAGE_TAG:=$(git rev-parse --short HEAD)-$(date +%s)}"

export STREAM_ETCD_REPLICAS=3
export STREAM_API_REPLICAS=2
export STREAM_HTTP_SOURCE_REPLICAS=2
export STREAM_STORAGE_READER_WRITER_REPLICAS=2
export STREAM_STORAGE_COORDINATOR_REPLICAS=2

export STREAM_ETCD_MEMORY_SOFT_LIMIT="200Mi"
export STREAM_ETCD_MEMORY_HARD_LIMIT="250Mi"
export STREAM_ETCD_CPU_SOFT_LIMIT="1000m"

export STREAM_HTTP_SOURCE_MEMORY_SOFT_LIMIT="800Mi"
export STREAM_HTTP_SOURCE_MEMORY_HARD_LIMIT="1000Mi"
export STREAM_HTTP_SOURCE_CPU_SOFT_LIMIT="1000m"

export STREAM_ETCD_STORAGE_CLASS_NAME="standard"
export STREAM_VOLUME_FAST_STORAGE_CLASS_NAME="standard"
export STREAM_VOLUME_MEDIUM_STORAGE_CLASS_NAME="standard"
export STREAM_VOLUME_SLOW_STORAGE_CLASS_NAME="standard"

export STREAM_VOLUME_FAST_STORAGE_SIZE="100Mi"
export STREAM_VOLUME_MEDIUM_STORAGE_SIZE="100Mi"
export STREAM_VOLUME_SLOW_STORAGE_SIZE="100Mi"


# Start minikube if needed
if ! minikube status > /dev/null; then
  ./../common/scripts/minikube/start.sh
fi

# Build Docker image in the local Docker, so it is cached, if Minikube is destroyed
SERVICE_IMAGE="$STREAM_IMAGE_REPOSITORY:$STREAM_IMAGE_TAG"
echo
echo "Building Service image ..."
echo "--------------------------"
docker build -t "$SERVICE_IMAGE" -f "./docker/service/Dockerfile" "../../"
echo

# Load the images to the Minikube
minikube image load --overwrite=true "$SERVICE_IMAGE"

# Build Docker image in the local Docker, so it is cached, if Minikube is destroyed
K6_IMAGE="$K6_IMAGE_REPOSITORY:$K6_IMAGE_TAG"
echo
echo "Building K6 image ..."
echo "--------------------------"
docker build -t "$K6_IMAGE" -f "./docker/k6/Dockerfile" "../../"
echo

minikube image load --overwrite=true "$K6_IMAGE"

echo
echo "Images in the MiniKube:"
echo "--------------------------"
minikube image list

# Set the required node label
minikube kubectl -- label nodes --overwrite --all nodepool=main > /dev/null

# Common part
echo
echo "Starting deployment ..."
echo "--------------------------"
. common.sh

# Local specific part of the deploy
kubectl apply -f ./kubernetes/deploy/cloud/local/service-api.yaml
kubectl apply -f ./kubernetes/deploy/cloud/local/service-http-source.yaml

# Wait for the deployment
. ./wait.sh

# Print info
echo
echo "To interact with the MiniKube profile run:"
echo "export MINIKUBE_PROFILE=${MINIKUBE_PROFILE}"
echo
echo "To clear the MiniKube:"
echo "MINIKUBE_PROFILE=${MINIKUBE_PROFILE} minikube delete --purge"
echo
echo "Run port-forwarding to access services:"
echo "API:"
echo "  kubectl port-forward --address 0.0.0.0 --namespace $NAMESPACE service/stream-api :80"
echo "  OR "
echo "HTTP source:"
echo "  kubectl port-forward --address 0.0.0.0 --namespace $NAMESPACE service/stream-http-source :80"
echo "  OR "
