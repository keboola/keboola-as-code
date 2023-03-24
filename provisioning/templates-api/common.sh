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
: ${ETCD_STORAGE_CLASS_NAME?"Missing ETCD_STORAGE_CLASS_NAME"}

# Constants
export NAMESPACE="templates-api"
ETCD_HELM_CHART_VERSION="8.5.8"

# Disable pod disruption budget (51%) if replicaCount=1, so it doesn't block the rollout.
TEMPLATES_API_ETCD_PDB_CREATE=$([[ $TEMPLATES_API_ETCD_REPLICAS -gt 1 ]] && echo 'true' || echo 'false')

# Common part of the deployment. Same for AWS/Azure/Local
./kubernetes/build.sh

# Namespace
kubectl apply -f ./kubernetes/deploy/namespace.yaml

# Get etcd root password, if it is already present
export ETCD_ROOT_PASSWORD=$(kubectl get secret --namespace "$NAMESPACE" templates-api-etcd -o jsonpath="{.data.etcd-root-password}" 2>/dev/null | base64 -d)

# TEMPORARY:
# Delete StatefulSet, keep pods, to resize the PVC disks.
kubectl delete sts --namespace "$NAMESPACE" "templates-api-etcd"
kubectl delete pvc --namespace "$NAMESPACE" "data-templates-api-etcd-0"

# Deploy etcd cluster
helm repo add --force-update bitnami https://charts.bitnami.com/bitnami
helm upgrade \
  --install templates-api-etcd bitnami/etcd \
  --version "$ETCD_HELM_CHART_VERSION" \
  --values ./kubernetes/deploy/etcd/values.yaml \
  --namespace "$NAMESPACE" \
  --set "replicaCount=$TEMPLATES_API_ETCD_REPLICAS" \
  --set "pdb.create=$TEMPLATES_API_ETCD_PDB_CREATE" \
  --set "auth.rbac.rootPassword=$ETCD_ROOT_PASSWORD" \
  --set "persistence.storageClass=$ETCD_STORAGE_CLASS_NAME"

# API
kubectl apply -f ./kubernetes/deploy/api/config-map.yaml
kubectl apply -f ./kubernetes/deploy/api/pdb.yaml
kubectl apply -f ./kubernetes/deploy/api/deployment.yaml
