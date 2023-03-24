### Buffer API Benchmarks

1. Start the server:
```
export BUFFER_API_PUBLIC_ADDRESS=http://localhost:10000
export BUFFER_API_STORAGE_API_HOST=connection.keboola.com
docker-compose run -u "$UID:$GID"  -p 10000:8000 --rm dev make run-buffer-api-once
```
2. Run the benchmark:
```
export API_TOKEN=<token>
export API_HOST=$BUFFER_API_PUBLIC_ADDRESS
docker-compose run -u "$UID:$GID" k6 run /scripts/k6/buffer-api/<name>
```

Where `<name>` is one of the following benchmark names:
- `static.js` - Receiver with a single export. Export only has static columns.
- `template.js` - Receiver with a single export. Export has a template column.

Available environment variables:

- `API_TOKEN` - Storage API Token (required)
- `API_HOST` - Buffer API host (default: `http://localhost:8001`)
- `K6_TIMEOUT` - max duration of the test (default: `60s`)
- `K6_ITERATIONS` - number of all requests (default: `10 000`)
- `K6_PARALLELISM` - number of workers that send requests (default: `1000`)
