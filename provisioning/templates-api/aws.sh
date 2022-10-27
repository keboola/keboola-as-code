#!/usr/bin/env bash
set -Eeuo pipefail

# Prevent direct run of the script
if [ "${BASH_SOURCE[0]}" -ef "$0" ]
then
    echo 'This script should not be executed directly, please run "deploy.sh" instead.'
    exit 1
fi

# Required ENVs
: ${AWS_REGION?"Missing AWS_REGION"}
: ${AWS_EKS_CLUSTER_NAME?"Missing AWS_EKS_CLUSTER_NAME"}

# Get credentials to the Amazon Elastic Kubernetes Service
aws eks update-kubeconfig --name "$AWS_EKS_CLUSTER_NAME" --region "$AWS_REGION"

# Common part of the deploy
. ./common.sh

# AWS specific part of the deploy
kubectl apply -f ./kubernetes/deploy/aws/service.yaml
kubectl apply -f ./kubernetes/deploy/aws/ingress.yaml
kubectl rollout status deployment/templates-api --namespace templates-api --timeout=900s

