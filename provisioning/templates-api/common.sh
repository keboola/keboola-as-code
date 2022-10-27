#!/usr/bin/env bash

# Common part of the deployment. Same for AWS/Azure
./provisioning/kubernetes/build.sh
kubectl apply -f ./provisioning/kubernetes/deploy/namespace.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/etcd.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/config-map.yaml
kubectl apply -f ./provisioning/kubernetes/deploy/templates-api.yaml
