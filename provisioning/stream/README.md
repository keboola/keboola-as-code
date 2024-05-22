# Provisioning of the Stream Service

## Directory Structure

Entrypoint:
- `deploy.sh` - entrypoint for the AWS and Azure deployments.
- `deploy_local.sh` - entrypoint for the local deployment.

Directories:
- `dev` - various auxiliary files for development.
- `docker` - docker image of the service.
- `kubernetes` - templates of the Kubernetes configurations.

Included files (they are not called directly):
- `common.sh` - main file for all deployments.
- `aws.sh` - deploy script for AWS stacks.
- `azure.sh` - deploy script for Azure stacks.
- `local.sh` - deploy script for a local MiniKube deployment.

## Production Deployment

TBD

## Local Deployment

### Docker

In most cases, it is enough to run the service locally via Docker.
```sh
docker compose run --rm -u "$UID:$GID" --service-ports dev base
make run-stream-service
```

Read more in [`docs/DEVELOPMENT.md`](../../docs/development.md).

### MiniKube

If you need to debug or test something in a Kubernetes cluster, you can use local deployment using [MiniKube](https://minikube.sigs.k8s.io/docs/start/).
```sh
./provisioning/stream/deploy_local.sh
```

At the end of the script, the URL of the service is printed.
```sh
To interact with the MiniKube profile run:
export MINIKUBE_PROFILE=stream

To clear the MiniKube:
MINIKUBE_PROFILE=stream minikube delete --purge

Load balancer of the service is accessible at:
http://172.17.0.2:32183
```

### etcd

Etcd deployment includes a network policy,
only pods with `stream-etcd-client=true` can connect to the etcd.

#### Client

If you need to start the etcd client, you can use this following commands.

Run interactive container:
```
export ETCD_ROOT_PASSWORD=$(kubectl get secret --namespace "stream" stream-etcd -o jsonpath="{.data.etcd-root-password}" 2>/dev/null | base64 -d)

kubectl run --tty --stdin --rm --restart=Never stream-etcd-client \
  --namespace stream \
  --image docker.io/bitnami/etcd:3.5.5-debian-11-r16 \
  --labels="stream-etcd-client=true" \
  --env="ETCD_ROOT_PASSWORD=$ETCD_ROOT_PASSWORD" \
  --env="ETCDCTL_ENDPOINTS=stream-etcd:2379" \
  --command -- bash
```

Use client inside container:
```
etcdctl --user root:$ETCD_ROOT_PASSWORD put /message Hello
etcdctl --user root:$ETCD_ROOT_PASSWORD get /message
```
