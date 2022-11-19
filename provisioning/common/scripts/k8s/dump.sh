#!/usr/bin/env bash
set -Eeuo pipefail

# Script dumps K8S objects to a JSON file, defined as the first argument.
if [ $# -ne 1 ]; then
  echo "Please specify one argument: path to the target file"
  exit 1
fi

# Arguments
TARGET_FILE="$1"

# Print all K8S resource types
echo "All found resource types:"
echo "----------------------------------"
kubectl api-resources --namespaced
echo "----------------------------------"
echo

# K8S resource types to dump, "all" doesn't includes all :)
# https://github.com/kubernetes/kubectl/issues/151
KINDS="
configmaps
endpoints
persistentvolumeclaims
pods
secrets
services
daemonsets
deployments
replicasets
statefulsets
horizontalpodautoscalers
cronjobs
networkpolicies
poddisruptionbudgets
"
echo "Resource types to dump:"
echo "----------------------------------"
echo "$KINDS"
echo "----------------------------------"
echo

# Print human readable list
KINDS_INLINE=$(echo "$KINDS" | sed -z -E 's/\n+/,/g;s/^,//;s/,$//')
echo "Kubernetes overview:"
echo "----------------------------------"
kubectl get "$KINDS_INLINE"
echo "----------------------------------"
echo

# Dump objects
kubectl get "$KINDS_INLINE" --ignore-not-found --sort-by='{.metadata.name}' --output json > "$TARGET_FILE"
