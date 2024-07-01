#!/usr/bin/env bash
set -Eeuo pipefail

echo
echo "Starting minikube ..."
echo "--------------------------"

# Enable Network Policies support.
#    Kindnet - default CNI, does not support Network Policies, by design.
#    https://minikube.sigs.k8s.io/docs/handbook/network_policy/
minikube start \
--wait "apiserver,system_pods,default_sa,apps_running,node_ready,kubelet" \
--cni=calico
