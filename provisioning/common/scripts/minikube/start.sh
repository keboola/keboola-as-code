#!/usr/bin/env bash
set -Eeuo pipefail

echo
echo "Starting minikube ..."
echo "--------------------------"

# Enable Network Policies support.
#    Kindnet - default CNI, does not support Network Policies, by design.
#    https://minikube.sigs.k8s.io/docs/handbook/network_policy/
minikube start \
--cpus 3 memory 2048 \
--extra-config=kubelet.housekeeping-interval=10s \
--wait "apiserver,system_pods,default_sa,apps_running,node_ready,kubelet" \
--cni=calico

minikube addons enable metrics-server
minikube addons enable csi-hostpath-driver
