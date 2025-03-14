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
export ETCD_STORAGE_CLASS_NAME=
# This will be replaced with Azure encryptor provider
export STREAM_ENCRYPTION_PROVIDER="azure"
. ./common.sh

# Azure specific part of the deploy
kubectl apply -f ./kubernetes/deploy/cloud/azure/service-api.yaml
kubectl apply -f ./kubernetes/deploy/cloud/azure/service-http-source.yaml
kubectl apply -f ./kubernetes/deploy/cloud/azure/sc-stream-api.yaml

# Wait for the rollout
. ./wait.sh

# Update API IP
API_IP_ADDRESS=""
TIME_WAITED=0
#shellcheck disable=2203
while [[ -z "$API_IP_ADDRESS" && $TIME_WAITED -lt 15*60 ]]; do # each 10s, max 15min
    echo "Waiting for Stream API ingress IP..."
    sleep 10;
    TIME_WAITED=$((TIME_WAITED + 10))
    API_IP_ADDRESS=$(kubectl get services \
      --selector "app=stream-api" \
      --namespace "$NAMESPACE" \
      --no-headers \
      --output jsonpath="{.items[0].status.loadBalancer.ingress[0].ip}")
done

# Update HTTP Source IP
HTTP_SOURCE_IP_ADDRESS=""
TIME_WAITED=0
#shellcheck disable=2203
while [[ -z "$HTTP_SOURCE_IP_ADDRESS" && $TIME_WAITED -lt 15*60 ]]; do # each 10s, max 15min
    echo "Waiting for Stream HTTP Source ingress IP..."
    sleep 10;
    TIME_WAITED=$((TIME_WAITED + 10))
    HTTP_SOURCE_IP_ADDRESS=$(kubectl get services \
      --selector "app=stream-http-source" \
      --namespace "$NAMESPACE" \
      --no-headers \
      --output jsonpath="{.items[0].status.loadBalancer.ingress[0].ip}")
done

APPLICATION_GATEWAY_NAME=$(az deployment group show \
  --resource-group "$RESOURCE_GROUP" \
  --name kbc-application-gateway \
  --query "properties.outputs.appGatewayName.value" \
  --output tsv)

az network application-gateway address-pool update \
  --gateway-name="$APPLICATION_GATEWAY_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --name=stream \
  --servers "$API_IP_ADDRESS"

az network application-gateway address-pool update \
  --gateway-name="$APPLICATION_GATEWAY_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --name=stream-in \
  --servers "$HTTP_SOURCE_IP_ADDRESS"

az network application-gateway probe update \
  --gateway-name="$APPLICATION_GATEWAY_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --name=stream-health-probe \
  --host "$API_IP_ADDRESS"

az network application-gateway probe update \
  --gateway-name="$APPLICATION_GATEWAY_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --name=stream-in-health-probe \
  --host "$HTTP_SOURCE_IP_ADDRESS"
