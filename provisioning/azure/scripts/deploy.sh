#!/usr/bin/env bash
set -Eeuo pipefail

CLUSTER_NAME=$(az deployment group show \
  --resource-group "$RESOURCE_GROUP" \
  --name kbc-aks \
  --query "properties.outputs.clusterName.value" \
  --output tsv)

az aks get-credentials --name "$CLUSTER_NAME" --resource-group "$RESOURCE_GROUP" --overwrite-existing

./provisioning/kubernetes/build.sh

# TEMPORARY
kubectl delete all -n default -l app=templates-api --ignore-not-found
kubectl delete configmap -n default templates-api --ignore-not-found
kubectl delete poddisruptionbudget -n default templates-api-pdb --ignore-not-found

kubectl apply -f ./provisioning/kubernetes/deploy/namespace.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/etcd.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/config-map.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/templates-api.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/azure/service.yaml
kubectl rollout status deployment/templates-api --namespace templates-api --timeout=900s

TEMPLATES_API_IP=""
TIME_WAITED=0
# every 10 seconds but in total max 15 minutes try to fetch TEMPLATES_API_IP
#shellcheck disable=2203
while [[ -z "$TEMPLATES_API_IP" && $TIME_WAITED -lt 15*60 ]]; do
    echo "Waiting for Templates API ingress IP..."
    sleep 10;
    TIME_WAITED=$((TIME_WAITED + 10))
    TEMPLATES_API_IP=$(kubectl get services \
      --selector "app=templates-api" \
      --namespace templates-api \
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
  --name=templates \
  --servers "$TEMPLATES_API_IP"

az network application-gateway probe update \
  --gateway-name="$APPLICATION_GATEWAY_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --name=templates-health-probe \
  --host "$TEMPLATES_API_IP"

