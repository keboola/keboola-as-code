#!/usr/bin/env bash
set -Eeuo pipefail

# Prevent direct run of the script
if [ "${BASH_SOURCE[0]}" -ef "$0" ]
then
    echo 'This script should not be executed directly, please run "deploy_local.sh" instead.'
    exit 1
fi

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

echo
echo "Images in the MiniKube:"
echo "--------------------------"
minikube image list

# Set the required node label
minikube kubectl -- label nodes --overwrite --all nodepool=main > /dev/null

# Common part
export ETCD_STORAGE_CLASS_NAME=
echo
echo "Starting deployment ..."
echo "--------------------------"
. common.sh

# Local specific part of the deploy
kubectl apply -f ./kubernetes/deploy/cloud/local/service.yaml

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
echo "Load balancer of the service is accessible at:"
minikube service --url --namespace "$NAMESPACE" stream-api

