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
  ./../common/scripts/start-minikube.sh
fi

# Build Docker image in the local Docker, so it is cached, if Minikube is destroyed
API_IMAGE="$BUFFER_API_REPOSITORY:$BUFFER_API_IMAGE_TAG"
WORKER_IMAGE="$BUFFER_WORKER_REPOSITORY:$BUFFER_WORKER_IMAGE_TAG"
echo
echo "Building API image ..."
echo "--------------------------"
docker build -t "$API_IMAGE" -f "./docker/api/Dockerfile" "../../"
echo
echo "Building Worker image ..."
echo "--------------------------"
docker build -t "$WORKER_IMAGE" -f "./docker/worker/Dockerfile" "../../"

# Load the images to the Minikube
minikube image load --overwrite=true "$API_IMAGE"
minikube image load --overwrite=true "$WORKER_IMAGE"

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
# TODO

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
