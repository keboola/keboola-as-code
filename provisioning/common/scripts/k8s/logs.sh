#!/usr/bin/env bash
set -Eeuo pipefail

if [ $# -ne 1 ]; then
  echo "Please specify one argument: target directory"
  exit 1
fi

# Arguments
TARGET_DIR="$1"

# Save logs for each pod to a separate file
podLogsDir="$TARGET_DIR/logs/pods"
mkdir -p "$podLogsDir"
echo "Saving pods logs:"
kubectl get pods --all-namespaces --no-header -o custom-columns=:metadata.namespace,:metadata.name | \
  xargs -L 1 --no-run-if-empty \
  sh -c '
  dir="$1"
  namespace="$2"
  pod="$3"
  path="${dir}/${namespace}#${pod}.log"
  echo "$path"
  kubectl logs "$pod" --namespace "$namespace" --ignore-errors --timestamps --prefix --all-containers > "$path"
  ' sh $podLogsDir || true

# Save events
eventsFile="$TARGET_DIR/logs/events.log"
echo "Saving events \"$eventsFile\""
kubectl get events  --all-namespaces --show-kind --sort-by='.metadata.creationTimestamp' \
  -o custom-columns=FirstSeen:.firstTimestamp,LastSeen:.lastTimestamp,Count:.count,From:.source.component,Namespace:.involvedObject.namespace,Kind:.involvedObject.kind,Object:.involvedObject.name,Type:.type,Reason:.reason,Message:.message \
  | tee "$eventsFile"
