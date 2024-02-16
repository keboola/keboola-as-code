#!/usr/bin/env bash

# Prevent direct run of the script
if [ "${BASH_SOURCE[0]}" -ef "$0" ]
then
    echo 'This script should not be executed directly, please run "deploy.sh" instead.'
    exit 1
fi

# Required ENVs
: ${RELEASE_ID?"Missing RELEASE_ID"}
: ${KEBOOLA_STACK?"Missing KEBOOLA_STACK"}
: ${HOSTNAME_SUFFIX?"Missing HOSTNAME_SUFFIX"}
: ${TEMPLATES_API_REPOSITORY?"Missing TEMPLATES_API_REPOSITORY"}
: ${TEMPLATES_API_IMAGE_TAG?"Missing TEMPLATES_API_IMAGE_TAG"}
: ${TEMPLATES_API_REPLICAS?"Missing TEMPLATES_API_REPLICAS"}

# Constants
export NAMESPACE="templates-api"

# Common part of the deployment. Same for AWS/Azure/Local
./kubernetes/build.sh

# Namespace
kubectl apply -f ./kubernetes/deploy/namespace.yaml

# API
kubectl apply -f ./kubernetes/deploy/api/config-map.yaml
kubectl apply -f ./kubernetes/deploy/api/pdb.yaml
kubectl apply -f ./kubernetes/deploy/api/network-policy.yaml
kubectl apply -f ./kubernetes/deploy/api/deployment.yaml
