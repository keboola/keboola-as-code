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
if kubectl rollout status sts/buffer-etcd --namespace "$NAMESPACE" --timeout=900s; then
  echo
  echo "Etcd deployment has been successful."
else
  echo
  echo "Etcd deployment failed."
  minikube kubectl -- logs --namespace "$NAMESPACE" --follow=false --timestamps=true --selector "app=buffer-etcd"
  exit 1
fi
