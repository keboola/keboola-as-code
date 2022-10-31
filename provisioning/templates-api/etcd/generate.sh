#!/usr/bin/env bash
set -Eeuo pipefail

cd "$(dirname "$0")"
helm repo add --force-update bitnami https://charts.bitnami.com/bitnami
helm template templates-api-etcd bitnami/etcd -f ./values.yaml -n templates-api > etcd.yaml

# Replace root password by ENV
sed  -i'.bak' -e's/etcd-root-password: ".*"/etcd-root-password: "$ETCD_ROOT_PASSWORD_BASE64"/g' etcd.yaml

# Replace replica count by ENV
sed  -i'.bak' -e's/replicas: .*/replicas: $TEMPLATES_API_ETCD_REPLICAS/g' etcd.yaml
sed  -i'.bak' -e's/value: "templates-api-etcd-0=.*"/value: "$ETCD_INITIAL_CLUSTER"/g' etcd.yaml

