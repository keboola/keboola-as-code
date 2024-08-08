export KEBOOLA_STACK=us-central1.gcp.keboola.dev
export API_TOKEN=$STORAGE_API_TOKEN
export API_TOKEN_BASE64=$(echo -n $API_TOKEN | base64)

# CD to the script directory
cd "$(dirname "$0")"

# TODO: if we want to push
#docker login --username keboolabot --password $DOCKER_REGISTRY_PASSWORD docker.io
#docker push keboolabot/stream-benchmark:latest
#docker logout

rm -fR deploy/benchmark
mkdir -p deploy/benchmark

envsubst < ./templates/benchmark/namespace.yaml > deploy/benchmark/namespace.yaml
envsubst < ./templates/benchmark/secret.yaml > deploy/benchmark/secret.yaml
envsubst < ./templates/benchmark/job.yaml > deploy/benchmark/job.yaml

kubectl apply -f ./deploy/benchmark/namespace.yaml
kubectl apply -f ./deploy/benchmark/secret.yaml
kubectl apply -f ./deploy/benchmark/job.yaml
