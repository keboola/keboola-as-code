# Templates API Etcd Cluster

## Overview

- Peers and clients communicate using the unencrypted HTTP/gRPC protocol.
- Communication is limited to participants using `NetworkPolicy`.
  - Client must have `templates-api-etcd-client: true` label.
  -  Clients connect to `templates-api-etcd.default.svc.cluster.local:2379`.
- Authentication is mandatory, there is only one user - `root`.
  - Root password is random, it is stored in `templates-api-etcd` secret under `etcd-root-password` key.
- Number of replicas is defined by `TEMPLATES_API_ETCD_REPLICAS` env variable (default `1`).
- To re-generate `provisioning/templates/etcd/yaml` run `./generate.sh` and copy the file.
- The number of nodes cannot be changed later (simple logic in `build.sh` is not enough).

## CLI Client For Testing

Run container:
```bash
export ETCD_ROOT_PASSWORD=$(kubectl get secret --namespace templates-api templates-api-etcd -o jsonpath="{.data.etcd-root-password}" | base64 -d)
```

```bash
kubectl run -i -t templates-api-etcd-client --restart=Never --rm \
  --namespace templates-api \
  --image docker.io/bitnami/etcd:3.5.4-debian-11-r27 \
  --labels="templates-api-etcd-client=true" \
  --env="ETCD_ROOT_PASSWORD=$ETCD_ROOT_PASSWORD" \
  --env="ETCDCTL_ENDPOINTS="templates-api-etcd.default.svc.cluster.local:2379"" \
  --command -- bash
```

Use client inside container:
```bash
etcdctl --user root:$ETCD_ROOT_PASSWORD put /message Hello
etcdctl --user root:$ETCD_ROOT_PASSWORD get /message
```

## Local Testing / Deploy

```bash
# Start k8s and add node label "nodepool: main" manually
minikube start
# Generate definitions by Helm
./provisioning/etcd/generate.sh && \
# Copy definitions
cp -f ./provisioning/etcd/etcd.yaml ./provisioning/kubernetes/templates/etcd.yaml && \
# Replace ENVs
CLOUD_PROVIDER=aws ./provisioning/kubernetes/build.sh && \
# Apply
kubectl apply -f ./provisioning/kubernetes/deploy/namespace.yaml && \
kubectl apply -f ./provisioning/kubernetes/deploy/etcd.yaml
```

To delete all resources use `minikube delete` or:
```bash
kubectl delete all,secret,pvc -l app=templates-api-etcd
```
