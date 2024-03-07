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
IMAGE="$APPS_PROXY_REPOSITORY:$APPS_PROXY_IMAGE_TAG"
echo
echo "Building Proxy image ..."
echo "--------------------------"
docker build -t "$IMAGE" -f "./docker/Dockerfile" "../../"

# Load the image to the Minikube
minikube image load --overwrite=true "$IMAGE"

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
minikube service --url --namespace "$NAMESPACE" apps-proxy
