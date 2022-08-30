#!/usr/bin/env bash
set -Eeuo pipefail

aws eks update-kubeconfig --name "$AWS_EKS_CLUSTER_NAME" --region "$AWS_REGION"
kubectl config set-context --current --namespace templates-api

./provisioning/kubernetes/build.sh

# TEMPORARY
kubectl delete all -n default -l app=templates-api --ignore-not-found
kubectl delete configmap -n default templates-api --ignore-not-found
kubectl delete ingress -n default templates-api --ignore-not-found

kubectl apply -f ./provisioning/kubernetes/deploy/namespace.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/config-map.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/templates-api.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/aws/service.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/aws/ingress.yaml
kubectl rollout status deployment/templates-api --timeout=900s

