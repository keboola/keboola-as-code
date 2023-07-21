### Buffer API Benchmarks

#### 1. Start API node

```
export BUFFER_API_STORAGE_API_HOST=connection.keboola.com
```

```
docker-compose run -u "$UID:$GID" --rm --name api-node \
benchmark-go make run-buffer-api-once
```

#### 2. Start Worker node

```
export BUFFER_WORKER_STORAGE_API_HOST=connection.keboola.com
```

```
docker-compose run -u "$UID:$GID" --rm --name worker-node \
benchmark-go make run-buffer-worker-once
```

#### 3. Run the benchmark

```
export BENCHMARK_API_TOKEN=<token>
export BENCHMARK_API_HOST=http://api-node:8001
```

```
docker-compose run -u "$UID:$GID" --rm --name k6 \
benchmark-k6 run --out statsd /benchmarks/buffer-api/cases/<name>
```

Where `<name>` is one of the following benchmark names:
- `static.js` - Receiver with a single export. Export only has static columns.
- `template.js` - Receiver with a single export. Export has a template column.

#### 4. Open dashboard or check summary in the console
```
http://localhost:19999
```