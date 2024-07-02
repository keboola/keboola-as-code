### Stream API Benchmarks

1. Start the server:
```
export STREAM_API_PUBLIC_ADDRESS=http://localhost:10000
export STREAM_API_STORAGE_API_HOST=connection.keboola.com
docker compose run -u "$UID:$GID"  -p 10000:8000 --rm dev make run-stream-api-once
```
2. Run the benchmark:
```
export API_TOKEN=<token>
export API_HOST=$STREAM_API_PUBLIC_ADDRESS
docker compose run -u "$UID:$GID" k6 run /scripts/k6/stream-api/<name>
```

Where `<name>` is one of the following benchmark names:
- `static.js` - Source with a signle sink. Sink only has static columns.
- `template.js` - source with a single sink. Sink has a template column.

Available environment variables:

- `API_TOKEN` - Storage API Token (required)
- `API_HOST` - Stream API host (default: `http://localhost:8001`)
- `K6_TIMEOUT` - max duration of the test (default: `60s`)
- `K6_ITERATIONS` - number of all requests (default: `10 000`)
- `K6_PARALLELISM` - number of workers that send requests (default: `1000`)
