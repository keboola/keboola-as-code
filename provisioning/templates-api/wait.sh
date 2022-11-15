#!/usr/bin/env bash
set -Eeuo pipefail

# Prevent direct run of the script
if [ "${BASH_SOURCE[0]}" -ef "$0" ]
then
    echo 'This script should not be executed directly, please run "deploy.sh" or "deploy_local.sh" instead.'
    exit 1
fi

# Wait for changes in the etcd cluster
echo
echo "Waiting for the etcd rollout ..."
echo "--------------------------"
if kubectl rollout status sts/templates-api-etcd --namespace "$NAMESPACE" --timeout=900s; then
  echo
  echo "Etcd deployment has been successful."
else
  echo
  echo "Etcd deployment failed."
  minikube kubectl -- logs --namespace "$NAMESPACE" --follow=false --timestamps=true --selector "app=templates-api-etcd"
  exit 1
fi

# Wait for the API deployment
echo
echo "Waiting for the API rollout ..."
echo "--------------------------"
if kubectl rollout status deployment/templates-api --namespace "$NAMESPACE" --timeout=900s; then
  echo
  echo "API deployment has been successful."
  echo "--------------------------"
else
  echo
  echo "API deployment failed."
  echo "--------------------------"
  minikube kubectl -- logs --namespace "$NAMESPACE" --follow=false --timestamps=true --selector "app=templates-api"
  exit 1
fi
