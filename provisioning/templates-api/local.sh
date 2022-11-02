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
    echo
    echo "Starting minikube ..."
    echo "--------------------------"
    # 1. Use v1.24 - https://github.com/kubernetes/minikube/issues/15173
    # 2. Enable Network Policies support.
    #    Kindnet - default CNI, does not support Network Policies, by design.
    #    https://minikube.sigs.k8s.io/docs/handbook/network_policy/
    minikube start \
    --wait "apiserver,system_pods,default_sa,apps_running,node_ready,kubelet" \
    --kubernetes-version=v1.24.7 \
    --network-plugin=cni --cni=calico
fi

# Build Docker image in the local Docker, so it is cached, if Minikube is destroyed
IMAGE="$TEMPLATES_API_REPOSITORY:$TEMPLATES_API_IMAGE_TAG"
echo
echo "Building API image ..."
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
kubectl apply -f ./kubernetes/deploy/local/service.yaml

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
minikube service --url --namespace templates-api templates-api
