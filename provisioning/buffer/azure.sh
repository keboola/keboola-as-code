#!/usr/bin/env bash
set -Eeuo pipefail

# Prevent direct run of the script
if [ "${BASH_SOURCE[0]}" -ef "$0" ]
then
    echo 'This script should not be executed directly, please run "deploy.sh" instead.'
    exit 1
fi

# Required ENVs
: ${RESOURCE_GROUP?"Missing RESOURCE_GROUP"}

# Get cluster name
CLUSTER_NAME=$(az deployment group show \
  --resource-group "$RESOURCE_GROUP" \
  --name kbc-aks \
  --query "properties.outputs.clusterName.value" \
  --output tsv)

# Get credentials to the Azure Managed Kubernetes Service
az aks get-credentials --name "$CLUSTER_NAME" --resource-group "$RESOURCE_GROUP" --overwrite-existing

# Common part of the deploy
. ./common.sh

# Azure specific part of the deploy
# TODO

# Wait for the rollout
. ./wait.sh

