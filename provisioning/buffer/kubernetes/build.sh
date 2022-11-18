#!/usr/bin/env bash
set -Eeuo pipefail

# CD to the script directory
cd "$(dirname "$0")"

# Namespace
envsubst < templates/namespace.yaml > deploy/namespace.yaml

# Etcd
cp ../../common/etcd/values.yaml deploy/etcd/values_common.yaml
cp templates/etcd.yaml deploy/etcd/values_buffer.yaml

# API
envsubst < templates/api/config-map.yaml > deploy/api/config-map.yaml
envsubst < templates/api/pdb.yaml        > deploy/api/pdb.yaml
envsubst < templates/api/deployment.yaml > deploy/api/deployment.yaml

# Worker
envsubst < templates/worker/config-map.yaml > deploy/worker/config-map.yaml
envsubst < templates/worker/pdb.yaml        > deploy/worker/pdb.yaml
envsubst < templates/worker/deployment.yaml > deploy/worker/deployment.yaml
