#!/usr/bin/env bash
set -Eeuo pipefail

# Prevent direct run of the script
if [ "${BASH_SOURCE[0]}" -ef "$0" ]
then
    echo 'This script should not be executed directly, please run "deploy.sh" or "deploy_local.sh" instead.'
    exit 1
fi

# Default values
KUBERNETES_ROLLOUT_WAIT="${KUBERNETES_ROLLOUT_WAIT:=1200s}"

# Wait for etcd rollout
echo
echo "Waiting for the etcd rollout ..."
echo "--------------------------"
if kubectl rollout status sts/buffer-etcd --namespace "$NAMESPACE" --timeout "$KUBERNETES_ROLLOUT_WAIT"; then
  echo
  echo "Etcd deployment has been successful."
else
  echo
  echo "Etcd deployment failed."
  echo "--------------------------"
  echo "Logs:"
  echo "--------------------------"
  kubectl logs --namespace "$NAMESPACE" --selector "app=buffer-etcd" --all-containers --prefix --timestamps  --tail=-1
  echo "--------------------------"
  exit 1
fi

# Wait for the API rollout
echo
echo "Waiting for the API rollout ..."
echo "--------------------------"
if kubectl rollout status deployment/buffer-api --namespace "$NAMESPACE" --timeout "$KUBERNETES_ROLLOUT_WAIT"; then
  echo
  echo "API deployment has been successful."
  echo "--------------------------"
else
  echo
  echo "API deployment failed."
  echo "--------------------------"
  echo "Logs:"
  echo "--------------------------"
  kubectl logs --namespace "$NAMESPACE" --selector "app=buffer-api" --all-containers --prefix --timestamps --tail=-1
  echo "--------------------------"
  exit 1
fi

# Wait for the Worker rollout
echo
echo "Waiting for the Worker rollout ..."
echo "--------------------------"
if kubectl rollout status deployment/buffer-worker --namespace "$NAMESPACE" --timeout "$KUBERNETES_ROLLOUT_WAIT"; then
  echo
  echo "Worker deployment has been successful."
  echo "--------------------------"
else
  echo
  echo "Worker deployment failed."
  echo "--------------------------"
  echo "Logs:"
  echo "--------------------------"
  kubectl logs --namespace "$NAMESPACE" --selector "app=buffer-worker" --all-containers --prefix --timestamps --tail=-1
  echo "--------------------------"
  exit 1
fi
