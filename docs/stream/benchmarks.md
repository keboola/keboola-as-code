### Stream API Benchmarks

1. Start the server:
```
export STREAM_NODE_ID=my-node
export STREAM_HOSTNAME=localhost
export STREAM_API_STORAGE_API_HOST=connection.keboola.com
export STREAM_API_LISTEN=0.0.0.0:10000
export STREAM_API_PUBLIC_URL=http://localhost:10000
export STREAM_SOURCE_HTTP_LISTEN=0.0.0.0:10001
export STREAM_SOURCE_HTTP_PUBLIC_URL=http://localhost:10001
export STREAM_STORAGE_VOLUMES_PATH=/tmp/k6/volumes
mkdir -p "$STREAM_STORAGE_VOLUMES_PATH/hdd/001"
docker compose run \
-u "$UID:$GID" \
-p 10000:10000 \
-p 10001:10001 \
-v "$STREAM_STORAGE_VOLUMES_PATH:$STREAM_STORAGE_VOLUMES_PATH" \
-e STREAM_NODE_ID \
-e STREAM_HOSTNAME \
-e STREAM_API_STORAGE_API_HOST \
-e STREAM_API_LISTEN \
-e STREAM_API_PUBLIC_URL \
-e STREAM_SOURCE_HTTP_LISTEN \
-e STREAM_SOURCE_HTTP_PUBLIC_URL \
-e STREAM_STORAGE_VOLUMES_PATH \
--rm \
dev make run-stream-service-once
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
