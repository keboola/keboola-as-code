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
: ${APPS_PROXY_REPOSITORY?"Missing APPS_PROXY_REPOSITORY"}
: ${APPS_PROXY_IMAGE_TAG?"Missing APPS_PROXY_IMAGE_TAG"}
: ${APPS_PROXY_REPLICAS?"Missing APPS_PROXY_REPLICAS"}
: ${APPS_PROXY_SANDBOXES_API_TOKEN?"Missing APPS_PROXY_SANDBOXES_API_TOKEN"}

# Constants
export NAMESPACE="apps-proxy"

# Common part of the deployment. Same for AWS/Azure/Local
./kubernetes/build.sh

# Namespace
kubectl apply -f ./kubernetes/deploy/namespace.yaml

if ! kubectl get secret apps-proxy-salt --namespace apps-proxy > /dev/null 2>&1; then
  APPS_PROXY_COOKIE_SECRET_SALT=$(head /dev/urandom | tr -dc A-Za-z0-9 | head -c 128)
  export APPS_PROXY_COOKIE_SECRET_SALT
  envsubst < kubernetes/templates/proxy/salt.yaml > kubernetes/deploy/proxy/salt.yaml
  kubectl apply -f ./kubernetes/deploy/proxy/salt.yaml
fi

if ! kubectl get secret apps-proxy-csrf-token-salt --namespace apps-proxy > /dev/null 2>&1; then
  APPS_PROXY_CSRF_TOKEN_SALT=$(head -c 32 /dev/urandom | openssl enc | hexdump -v -e '/1 "%02x"' -n 32)
  export APPS_PROXY_CSRF_TOKEN_SALT
  envsubst < kubernetes/templates/proxy/csrf-salt.yaml > kubernetes/deploy/proxy/csrf-salt.yaml
  kubectl apply -f ./kubernetes/deploy/proxy/csrf-salt.yaml
fi

# Proxy
kubectl apply -f ./kubernetes/deploy/proxy/token.yaml
kubectl apply -f ./kubernetes/deploy/proxy/config-map.yaml
kubectl apply -f ./kubernetes/deploy/proxy/pdb.yaml
kubectl apply -f ./kubernetes/deploy/proxy/network-policy.yaml
kubectl apply -f ./kubernetes/deploy/proxy/deployment.yaml

# CSAS migration dataaps users redirector
export CSAS_MIGRATION_APPS_PROXY_REDIRECT_REPLICAS="${CSAS_MIGRATION_APPS_PROXY_REDIRECT_REPLICAS:=0}"
if [ "$CSAS_MIGRATION_APPS_PROXY_REDIRECT_REPLICAS" -gt 0 ]; then
  kubectl apply -f ./kubernetes/deploy/nginx-redir/config-map.yaml
  kubectl apply -f ./kubernetes/deploy/nginx-redir/deployment.yaml
fi
