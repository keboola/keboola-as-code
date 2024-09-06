#!/usr/bin/env bash
set -Eeuo pipefail

if [ $# -ne 1 ]; then
  echo "Please specify one argument: target directory"
  exit 1
fi

# Arguments
TARGET_DIR="$1/logs/minikube"

mkdir -p "$TARGET_DIR"
statusPath="$TARGET_DIR/status.log"
logsPath="$TARGET_DIR/logs.log"

#echo "Saving minikube status \"$statusPath\""
#minikube status > "$statusPath"

echo "Saving minikube logs \"$logsPath\""
minikube logs   > "$logsPath"
