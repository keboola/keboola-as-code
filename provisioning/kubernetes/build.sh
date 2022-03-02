#!/usr/bin/env bash
set -Eeuo pipefail

envsubst < provisioning/kubernetes/templates/config-map.yaml > provisioning/kubernetes/deploy/config-map.yaml
envsubst < provisioning/kubernetes/templates/templates-api.yaml > provisioning/kubernetes/deploy/templates-api.yaml
envsubst < provisioning/kubernetes/templates/"$CLOUD_PROVIDER"/service.yaml > provisioning/kubernetes/deploy/"$CLOUD_PROVIDER"/service.yaml

if [[ "$CLOUD_PROVIDER" == "aws" ]]; then
  envsubst < provisioning/kubernetes/templates/aws/ingress.yaml > provisioning/kubernetes/deploy/aws/ingress.yaml
fi
