#!/usr/bin/env bash
set -Eeuo pipefail

# CD to the script directory
cd "$(dirname "$0")"

aws eks update-kubeconfig --name "$AWS_EKS_CLUSTER_NAME" --region "$AWS_REGION"

# Common part
./common.sh

# AWS specific
kubectl apply -f ./kubernetes/deploy/aws/service.yaml
kubectl apply -f ./kubernetes/deploy/aws/ingress.yaml
kubectl rollout status deployment/templates-api --namespace templates-api --timeout=900s

