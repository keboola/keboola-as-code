#!/usr/bin/env bash
set -Eeuo pipefail

aws eks update-kubeconfig --name "$AWS_EKS_CLUSTER_NAME" --region "$AWS_REGION"
kubectl config set-context --current --namespace default

./provisioning/kubernetes/build.sh

kubectl apply -f ./provisioning/kubernetes/deploy/config-map.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/templates-api.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/aws/service.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/aws/ingress.yaml
kubectl rollout status deployment/templates-api --timeout=900s

