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

# Wait for the Proxy rollout
echo
echo "Waiting for the Proxy rollout ..."
echo "--------------------------"
if kubectl rollout status deployment/apps-proxy --namespace "$NAMESPACE" --timeout "$KUBERNETES_ROLLOUT_WAIT"; then
  echo
  echo "Proxy deployment has been successful."
  echo "--------------------------"
else
  echo
  echo "Proxy deployment failed."
  echo "--------------------------"
  echo "Logs:"
  echo "--------------------------"
  kubectl logs --namespace "$NAMESPACE" --selector "app=apps-proxy" --all-containers --prefix --timestamps --tail=-1
  echo "--------------------------"
  exit 1
fi
