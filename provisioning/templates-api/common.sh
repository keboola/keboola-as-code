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

# Constants
ETCD_HELM_CHART_VERSION="8.5.8"

# Common part of the deployment. Same for AWS/Azure/Local
./kubernetes/build.sh

# Namespace
kubectl apply -f ./kubernetes/deploy/namespace.yaml

# Get etcd root password, if it is already present
export ETCD_ROOT_PASSWORD=$(kubectl get secret --namespace "templates-api" templates-api-etcd -o jsonpath="{.data.etcd-root-password}" 2>/dev/null | base64 -d)

# Deploy etcd cluster
helm repo add --force-update bitnami https://charts.bitnami.com/bitnami
helm upgrade \
  --install templates-api-etcd bitnami/etcd \
  --version "$ETCD_HELM_CHART_VERSION" \
  --values ./kubernetes/deploy/etcd/values.yaml \
  --namespace templates-api \
  --set "auth.rbac.rootPassword=$ETCD_ROOT_PASSWORD"

# Deploy templates API
kubectl apply -f ./kubernetes/deploy/config-map.yaml
kubectl apply -f ./kubernetes/deploy/templates-api.yaml
