# Provisioning of the Stream Service

## Components

- The Stream service is compiled to a single binary using `task build-stream-service`.
- There is also a single `docker/service/Dockerfile`.
- The command line `args` defines which service components are started, at least one component must be specified.
- All components may run in single process, but for scalability and resource management, they run in several pods.
- For curiosity's sake: etcd database can also be started within the same process, it can be [embedded](https://github.com/etcd-io/etcd/blob/main/server/embed/etcd.go).

### Components List

- `api` - Configuration API reads and writes stream configurations to etcd, waits for resources to be created in the Storage API.
- `http-source` - HTTP source receives records by a HTTP server, converts records to CSV and compresses them.
- `storage-writer` - Storage writer receives compressed CSV bytes and stores them to the local disk.
- `storage-reader` - Storage reader uploads slices from the local disk to a staging storage.
- `storage-coordinator` - Storage coordinator watches statistics and based on them, triggers:
  - Slice upload from the local disk to the staging storage.
  - File (with all slices) import from the staging storage to the target table.
  - The coordinator is modifying the state of the entity in the etcd and responsible component performs the action.

## Pods

![image](../../internal/pkg/service/stream/storage/level/local/volume/volume.svg)

### Pods List

- `stream-api`
  - Memory/CPU usage is minimal.
- `stream-http-source`
  - Memory/CPU usage increases with requests rate.
- `stream-storage-writer-reader`
  - Sufficient memory size and disks speed are required.
- `stream-storage-coordinator`
  - Memory/CPU usage is minimal.

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
- `gcp.sh` - deploy script for GCP stacks.
- `local.sh` - deploy script for a local MiniKube deployment.

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

## Testing Stream Service using k6/bash

In case you would like to test stream service using minikube, run the
```sh
./provisioning/stream/rollout-stream.sh
```

It sets up the service config map loadbalancer. You can check service loadbalancer in minikube
```sh
export MINIKUBE_PROFILE=stream
minikube service -n stream --all
```

It copies both API and HTTP source ports as we use it within the configuration.
Go to lens configuration and check the values of source.http.publicUrl and api.publicUrl.

It restarts both deployments, so the new configuration is applied.

```sh
To interact with the MiniKube profile run:
export MINIKUBE_PROFILE=stream

To clear the MiniKube:
MINIKUBE_PROFILE=stream minikube delete --purge
```

### API

To access the API you need a token from Keboola. By default, connection.keboola.com stack is used.
Then you need the urls of the local Stream API and HTTP source, both are in the output of `rollout-stream.sh`.
```sh
export KEBOOLA_TOKEN=<token>
export KEBOOLA_STREAM_API_URL=<url>
export KEBOOLA_STREAM_SOURCE_URL=<url>
```

Then you can create a source:
```sh
curl --request POST \
     --header "Content-Type: application/json" \
     --header "X-StorageApi-Token: $KEBOOLA_TOKEN" \
     --data-binary '{
       "sourceId": "test-source-1",
       "type": "http",
       "name": "Test Source 1"
     }' \
"$KEBOOLA_STREAM_API_URL/v1/branches/default/sources"
```

And a sink:
```sh
curl --request POST \
     --header "Content-Type: application/json" \
     --header "X-StorageApi-Token: $KEBOOLA_TOKEN" \
     --data-binary '{
       "sinkId": "test-sink-1",
       "type": "table",
       "name": "Raw Data Sink",
       "description": "The sink stores records to a table.",
       "table": {
         "type": "keboola",
         "tableId": "in.c-bucket.table",
         "mapping": {
           "columns": [
             {
               "type": "uuid",
               "name": "id"
             },
             {
               "type": "path",
               "name": "data",
               "path": "data"
             }
           ]
         }
       }
     }' \
"$KEBOOLA_STREAM_API_URL/v1/branches/default/sources/test-source-1/sinks"
```

The curl command to create a source returns a task URL which you can use to get the source url. And from that url you can obtain the url where the stream accepts data.

Then you can send data to the stream:

```sh
curl --request POST \
     --header "Content-Type: application/json"
     --data-binary '{
       "data": "Example data body"
     }' \
<stream_url>
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
  --image docker.io/bitnamilegacy/etcd:3.6.2-debian-12-r1 \
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
