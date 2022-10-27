#!/usr/bin/env bash
set -Eeuo pipefail

aws eks update-kubeconfig --name "$AWS_EKS_CLUSTER_NAME" --region "$AWS_REGION"

# Common part
. common.sh

# AWS specific
kubectl apply -f ./provisioning/kubernetes/deploy/aws/service.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/aws/ingress.yaml
kubectl rollout status deployment/templates-api --namespace templates-api --timeout=900s

