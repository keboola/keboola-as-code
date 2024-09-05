# Stream API Benchmarks

## Start the server

```sh
export STREAM_COMPONENTS="api http-source storage-writer storage-reader storage-coordinator"
export STREAM_ETCD_NAMESPACE=stream-bench-001
export STREAM_DEBUG_LOG=false
export STREAM_NODE_ID=my-node
export STREAM_HOSTNAME=localhost
export STREAM_STORAGE_API_HOST=connection.keboola.com
export STREAM_API_LISTEN=0.0.0.0:10000
export STREAM_API_PUBLIC_URL=http://localhost:10000
export STREAM_SOURCE_HTTP_LISTEN=0.0.0.0:10001
export STREAM_SOURCE_HTTP_PUBLIC_URL=http://localhost:10001
export STREAM_STORAGE_VOLUMES_PATH=/tmp/k6/volumes
export STREAM_PPROF_ENABLED=false
export STREAM_PPROF_LISTEN="0.0.0.0:4000"
mkdir -p "$STREAM_STORAGE_VOLUMES_PATH/hdd/001"
docker compose run \
    --rm \
    -u "$UID:$GID" \
    -p 4000:4000 \
    -p 10000:10000 \
    -p 10001:10001 \
    -v "$STREAM_STORAGE_VOLUMES_PATH:$STREAM_STORAGE_VOLUMES_PATH" \
    -e STREAM_ETCD_NAMESPACE \
    -e STREAM_DEBUG_LOG \
    -e STREAM_NODE_ID \
    -e STREAM_HOSTNAME \
    -e STREAM_STORAGE_API_HOST \
    -e STREAM_API_LISTEN \
    -e STREAM_API_PUBLIC_URL \
    -e STREAM_SOURCE_HTTP_LISTEN \
    -e STREAM_SOURCE_HTTP_PUBLIC_URL \
    -e STREAM_STORAGE_VOLUMES_PATH \
    -e STREAM_PPROF_ENABLED \
    -e STREAM_PPROF_LISTEN \
dev bash -c "go run ./cmd/stream/main.go -- $STREAM_COMPONENTS | jl"
```

### Notes

- Minimal `STREAM_COMPONENTS`: `api http-source storage-writer`
- All `STREAM_COMPONENTS`: `api http-source storage-writer storage-reader storage-coordinator`
- Use `jl -format logfmt` to show all log fields.
- Monitor also system resource, for example via `htop`, use
  - `F5` to enable tree view
  - `F4` to filter the list

## Run the benchmark

```sh
export API_TOKEN=<token>
export API_HOST=$STREAM_API_PUBLIC_URL
docker compose run --rm -u "$UID:$GID" k6 run /scripts/k6/stream-api/<name>
```

Where `<name>` is one of the following benchmark names:
- `static.js` - Source with a single sink. Sink only has static columns.
- `template.js` - source with a single sink. Sink has a template column.

Available environment variables:

- `API_TOKEN` - Storage API Token (required)
- `API_HOST` - Stream API host (default: `http://localhost:8001`)
- `K6_TIMEOUT` - max duration of the test (default: `60s`)
- `K6_ITERATIONS` - number of all requests (default: `10 000`)
- `K6_PARALLELISM` - number of workers that send requests (default: `1000`)

## Profiling

### PProf Profiling

PProf can be used to profile the application locally or on a testing stack.

Enable PProf profiler via ENVs, flags or a config file:
```sh
export STREAM_PPROF_ENABLED=true
....
```

Start stream service in the background, see "Start the server" above.

PProf profiles are then served on `http://localhost:4000/debug/pprof/`.

For example, you can see active goroutines:
```
http://localhost:4000/debug/pprof/goroutine?debug=2
```

#### PProf Visualization

Start a Go container to run `go tool` commands:
```
docker run --rm -u "$UID:$GID" -it --net host keboolabot/keboola-as-code-dev bash
```

Use `go tool pprof` to visualise profiles in a web browser:
```
cpu:       go tool pprof -http=0.0.0.0:4001 "http://localhost:4000/debug/pprof/profile?seconds=10"
memory:    go tool pprof -http=0.0.0.0:4001 "http://localhost:4000/debug/pprof/heap?seconds=10"
block:     go tool pprof -http=0.0.0.0:4001 "http://localhost:4000/debug/pprof/block?seconds=10"
mutex:     go tool pprof -http=0.0.0.0:4001 "http://localhost:4000/debug/pprof/mutex?seconds=10"
goroutine: go tool pprof -http=0.0.0.0:4001 "http://localhost:4000/debug/pprof/goroutine?seconds=10"
```

Open `http://localhost:4001` to see the profile visualisation.

#### Trace Visualization

Start a Go container to run `go tool` commands:
```
docker run --rm -it --net host keboolabot/keboola-as-code-dev bash
```

Download trace profile:
```sh
curl -s -o /tmp/trace.out "http://localhost:4000/debug/pprof/trace?seconds=10"
```

Use `go tool trace` to visualise the trace profile:
```sh
go tool trace -http=0.0.0.0:4001 /tmp/trace.out
```

Open `http://localhost:4001` to see the trace visualisation.
- Goroutines analysis: http://localhost:4001/goroutines
- Network blocking profile: http://localhost:4001/io
- Synchronization blocking profile: http://localhost:4001/block
- Syscall profile: http://localhost:4001/syscall
- Scheduler latency profile: http://localhost:4001/sched
- ...

#### Stop Service

Stop stream service in the background:
```sh
kill $(jobs -p)
```


### Datadog Profiling

Datadog can be used to profile the application on a testing stack.

Enable Datadog profiler via ENVs, flags or a config file:
```sh
export STREAM_DATADOG_PROFILER_ENABLED=true
export STREAM_DATADOG_PROFILER_CPU=true
export STREAM_DATADOG_PROFILER_MEMORY=true
export STREAM_DATADOG_PROFILER_BLOCK=true     # may have big overhead
export STREAM_DATADOG_PROFILER_MUTEX=true     # may have big overhead
export STREAM_DATADOG_PROFILER_GOROUTINE=true # may have big overhead
```

Check results in the Datadog.
