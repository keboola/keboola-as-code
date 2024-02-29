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
: ${APP_PROXY_REPOSITORY?"Missing APP_PROXY_REPOSITORY"}
: ${APP_PROXY_IMAGE_TAG?"Missing APP_PROXY_IMAGE_TAG"}
: ${APP_PROXY_REPLICAS?"Missing APP_PROXY_REPLICAS"}
: ${APP_PROXY_SANDBOXES_API_TOKEN?"Missing APP_PROXY_SANDBOXES_API_TOKEN"}

# Constants
export NAMESPACE="app-proxy"

# Common part of the deployment. Same for AWS/Azure/Local
./kubernetes/build.sh

# Namespace
kubectl apply -f ./kubernetes/deploy/namespace.yaml

if ! kubectl get secret app-proxy-salt --namespace app-proxy > /dev/null 2>&1; then
  COOKIE_SECRET_SALT=$(head /dev/urandom | tr -dc A-Za-z0-9 | head -c 128)
  export COOKIE_SECRET_SALT
  envsubst < kubernetes/templates/proxy/salt.yaml > kubernetes/deploy/proxy/salt.yaml
  kubectl apply -f ./kubernetes/deploy/proxy/salt.yaml
fi

# Proxy
kubectl apply -f ./kubernetes/deploy/proxy/secret.yaml
kubectl apply -f ./kubernetes/deploy/proxy/config-map.yaml
kubectl apply -f ./kubernetes/deploy/proxy/pdb.yaml
kubectl apply -f ./kubernetes/deploy/proxy/network-policy.yaml
kubectl apply -f ./kubernetes/deploy/proxy/deployment.yaml
