## Buffer API Benchmarks

### Testing on a local machine 

#### 1. Clear containers

```
docker-compose rm -vfs
```

#### 2. Start API node

```
export BUFFER_API_STORAGE_API_HOST=connection.keboola.com
export BUFFER_API_RECEIVER_BUFFER_SIZE=1G
```

```
docker-compose run -u "$UID:$GID" --rm --name api-node \
benchmark-go make run-buffer-api-once
```

#### 3. Start Worker node

```
export BUFFER_WORKER_STORAGE_API_HOST=connection.keboola.com
```

```
docker-compose run -u "$UID:$GID" --rm --name worker-node \
benchmark-go make run-buffer-worker-once
```

#### 4. Run the benchmark

```
export BENCHMARK_API_TOKEN=<token>
export BENCHMARK_API_HOST=http://api-node:8001
```

```
export BENCHMARK_MAX_VIRTUAL_USERS=100
export BENCHMARK_PARALLEL_REQS_PER_USER=10
export BENCHMARK_RAMPING_DURATION=2m
export BENCHMARK_STABLE_RATE_DURATION=2m
```

```
docker-compose run -u "$UID:$GID" --rm --name k6 \
benchmark-k6 run --out statsd /benchmarks/buffer-api/cases/<name>
```

Where `<name>` is one of the benchmarks in the `cases` directory, for example `static.js`.

#### 4. Open dashboard or check summary in the console
```
http://localhost:19999
```

### Local machine to a remote stack

See **4. Run the benchmark** in the **Testing on a local machine** above.

### Benchmark as Kubernetes job

Build docker image:
```
docker login --username keboolabot --password <1Password> docker.io
docker build -f ./provisioning/benchmark/docker/Dockerfile -t keboolabot/buffer-benchmark:latest .
docker push keboolabot/buffer-benchmark:latest 
```

Generate files:
```
export CREATE_BUFFER_BENCHMARK=true
export CLOUD_PROVIDER=aws
export KEBOOLA_STACK=eu-west-1.aws.keboola.dev
export HOSTNAME_SUFFIX=eu-west-1.aws.keboola.dev
export BENCHMARK_API_TOKEN=.....

./provisioning/buffer/kubernetes/build.sh

kubectl apply -f ./kubernetes/deploy/benchmark/namespace.yaml
kubectl apply -f ./kubernetes/deploy/benchmark/secret.yaml
kubectl apply -f ./kubernetes/deploy/benchmark/job.yaml
```