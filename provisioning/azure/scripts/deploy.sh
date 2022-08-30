#!/usr/bin/env bash
set -Eeuo pipefail

CLUSTER_NAME=$(az deployment group show \
  --resource-group "$RESOURCE_GROUP" \
  --name kbc-aks \
  --query "properties.outputs.clusterName.value" \
  --output tsv)

az aks get-credentials --name "$CLUSTER_NAME" --resource-group "$RESOURCE_GROUP" --overwrite-existing
kubectl config set-context --current --namespace templates-api

./provisioning/kubernetes/build.sh

# TEMPORARY
kubectl delete all -n default -l app=templates-api --ignore-not-found
kubectl delete configmap -n default templates-api --ignore-not-found

kubectl apply -f ./provisioning/kubernetes/deploy/namespace.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/config-map.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/templates-api.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/azure/service.yaml
kubectl rollout status deployment/templates-api --timeout=900s

TEMPLATES_API_IP=$(kubectl get services \
  --selector "app=templates-api" \
  --namespace templates-api \
  --no-headers \
  --output jsonpath="{.items[0].status.loadBalancer.ingress[0].ip}")

APPLICATION_GATEWAY_NAME=$(az deployment group show \
  --resource-group "$RESOURCE_GROUP" \
  --name kbc-application-gateway \
  --query "properties.outputs.appGatewayName.value" \
  --output tsv)

az network application-gateway address-pool update \
  --gateway-name="$APPLICATION_GATEWAY_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --name=templates \
  --servers "$TEMPLATES_API_IP"

az network application-gateway probe update \
  --gateway-name="$APPLICATION_GATEWAY_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --name=templates-health-probe \
  --host "$TEMPLATES_API_IP"

