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
export ETCD_STORAGE_CLASS_NAME="etcd-gp3-max"
. ./common.sh

# AWS specific part of the deploy
kubectl apply -f ./kubernetes/deploy/cloud/aws/service.yaml
kubectl apply -f ./kubernetes/deploy/cloud/aws/ingress.yaml

# Wait for the rollout
. ./wait.sh
