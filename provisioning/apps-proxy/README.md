# Provisioning of the Apps Proxy

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

## Production Deployment

- A tag `apps-proxy-vX.Y.Z` triggers GitHub Workflow.
- The `build-and-push-apps-proxy` step builds the Apps Proxy image and pushes it to a repository in AWS and Azure.
- Push to Azure ACR triggers a release pipeline.
- Links to the AWS and Azure pipelines can be found in the `Service` page, in the internal Confluence.

## Testing Deployment

- A tag `apps-proxy-vX.Y.Z-[a-z]+\.[0-9]+` triggers GitHub actions Workflow.
- The `build-and-push-apps-proxy` step builds the Apps Proxy image and pushes it to a repository in AWS and Azure.
- Push to Azure ACR triggers a testing pipeline.
- Links to the AWS and Azure pipelines can be found in the `Service` page, in the internal Confluence.

## Local Deployment

### Docker

In most cases, it is enough to run the service locally via Docker.
```sh
docker compose run --rm -u "$UID:$GID" --service-ports dev base
make run-apps-proxy
```

Read more in [`docs/DEVELOPMENT.md`](../../docs/development.md).

### MiniKube

If you need to debug or test something in a Kubernetes cluster, you can use local deployment using [MiniKube](https://minikube.sigs.k8s.io/docs/start/).
```sh
./provisioning/apps-proxy/deploy_local.sh
```

At the end of the script, the URL of the service is printed.
```sh
To interact with the MiniKube profile run:
export MINIKUBE_PROFILE=apps-proxy

To clear the MiniKube:
MINIKUBE_PROFILE=apps-proxy minikube delete --purge

Load balancer of the service is accessible at:
http://172.17.0.2:32183
```
