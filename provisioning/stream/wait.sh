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
if kubectl rollout status sts/stream-etcd --namespace "$NAMESPACE" --timeout "$KUBERNETES_ROLLOUT_WAIT"; then
  echo
  echo "Etcd deployment has been successful."
else
  echo
  echo "Etcd deployment failed."
  echo "--------------------------"
  echo "Logs:"
  echo "--------------------------"
  kubectl logs --namespace "$NAMESPACE" --selector "app=stream-etcd" --all-containers --prefix --timestamps  --tail=-1
  echo "--------------------------"
  exit 1
fi

# Wait for the API rollout
echo
echo "Waiting for the API rollout ..."
echo "--------------------------"
if kubectl rollout status deployment/stream-api --namespace "$NAMESPACE" --timeout "$KUBERNETES_ROLLOUT_WAIT"; then
  echo
  echo "API deployment has been successful."
  echo "--------------------------"
else
  echo
  echo "API deployment failed."
  echo "--------------------------"
  echo "Logs:"
  echo "--------------------------"
  kubectl logs --namespace "$NAMESPACE" --selector "app=stream-api" --all-containers --prefix --timestamps --tail=-1
  echo "--------------------------"
  exit 1
fi

# Wait for the HTTP source rollout
echo
echo "Waiting for the HTTP source rollout ..."
echo "--------------------------"
if kubectl rollout status deployment/stream-http-source --namespace "$NAMESPACE" --timeout "$KUBERNETES_ROLLOUT_WAIT"; then
  echo
  echo "HTTP source deployment has been successful."
  echo "--------------------------"
else
  echo
  echo "HTTP source deployment failed."
  echo "--------------------------"
  echo "Logs:"
  echo "--------------------------"
  kubectl logs --namespace "$NAMESPACE" --selector "app=stream-http-source" --all-containers --prefix --timestamps --tail=-1
  echo "--------------------------"
  exit 1
fi

# Wait for the Storage writer/reader rollout
echo
echo "Waiting for the Storage writer/reader rollout ..."
echo "--------------------------"
if kubectl rollout status statefulset/stream-storage-writer-reader --namespace "$NAMESPACE" --timeout "$KUBERNETES_ROLLOUT_WAIT"; then
  echo
  echo "Storage writer/reader rollout has been successful."
  echo "--------------------------"
else
  echo
  echo "Storage writer/reader rollout failed."
  echo "--------------------------"
  echo "Logs:"
  echo "--------------------------"
  kubectl logs --namespace "$NAMESPACE" --selector "app=stream-storage-writer-reader" --all-containers --prefix --timestamps --tail=-1
  echo "--------------------------"
  exit 1
fi

# Wait for the Storage coordinator rollout
echo
echo "Waiting for the Storage coordinator rollout ..."
echo "--------------------------"
if kubectl rollout status  deployment/stream-storage-coordinator --namespace "$NAMESPACE" --timeout "$KUBERNETES_ROLLOUT_WAIT"; then
  echo
  echo "Storage coordinator rollout has been successful."
  echo "--------------------------"
else
  echo
  echo "Storage coordinator rollout failed."
  echo "--------------------------"
  echo "Logs:"
  echo "--------------------------"
  kubectl logs --namespace "$NAMESPACE" --selector "app=stream-storage-coordinator" --all-containers --prefix --timestamps --tail=-1
  echo "--------------------------"
  exit 1
fi
