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
: ${TEMPLATES_API_ETCD_REPLICAS?"Missing TEMPLATES_API_ETCD_REPLICAS"}

# Common part of the deployment. Same for AWS/Azure
./kubernetes/build.sh
kubectl apply -f ./kubernetes/deploy/namespace.yaml
kubectl apply -f ./kubernetes/deploy/etcd.yaml
kubectl apply -f ./kubernetes/deploy/config-map.yaml
kubectl apply -f ./kubernetes/deploy/templates-api.yaml
