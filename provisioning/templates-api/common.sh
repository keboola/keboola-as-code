#!/usr/bin/env bash

# CD to the script directory
cd "$(dirname "$0")"

# Common part of the deployment. Same for AWS/Azure
./kubernetes/build.sh
kubectl apply -f ./kubernetes/deploy/namespace.yaml
kubectl apply -f ./kubernetes/deploy/etcd.yaml
kubectl apply -f ./kubernetes/deploy/config-map.yaml
kubectl apply -f ./kubernetes/deploy/templates-api.yaml
