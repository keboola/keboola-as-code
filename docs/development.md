# Development

## Quick Start

### Clone

Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/keboola-as-code
cd keboola-as-code
docker compose build
```

### Setup ENV

Create `.env` file with definition of testing projects:
```
TEST_KBC_PROJECTS='[{"host":"connection.keboola.com","project":1234,"stagingStorage":"s3","backend":"snowflake/bigquery","token":"<token>"},...]'
```

Staging storage can be `s3`, `abs` or `gcs`, according to the stack.

### Start Dev Container

Start an interactive console in a container, run:
```
docker compose run --rm -u "$UID:$GID" --service-ports dev bash
```

### Troubleshooting

Strange issues are usually caused by incorrect permissions or file owner somewhere. Here are some tips:

In case you're using Docker Desktop on Linux remove the `-u "$UID:$GID"`. It can cause some permission issues.

Run `sudo chown -R <user>:<group> ./` to reset owner on all files.

Run `docker volume ls` and `docker volume rm <volume>` to delete the named cache volume.

### Run Tests

Run the test suite and download the dependencies using:
```
make ci
```

To run only the CLI [E2E tests](./e2e_tests.md), you can use:
```
make tests-cli
```

Use the `TEST_VERBOSE=true` ENV to run tests with verbose output to see HTTP requests, ENVs, etc.:
```
TEST_VERBOSE=true go test -race -v -p 1 ./path/to/pkg... -run TestName/SubTest
```

Use the `TEST_HTTP_CLIENT_VERBOSE=true` ENV to dump all executed HTTP requests and their responses:
```
TEST_HTTP_CLIENT_VERBOSE=true TEST_VERBOSE=true go test -race -v -p 1 ./path/to/pkg... -run TestName/SubTest
```

Use the `ETCD_VERBOSE=true` ENV to dump all `etcd` operations:
```
ETCD_VERBOSE=true TEST_VERBOSE=true go test -race -v -p 1 ./path/to/pkg... -run TestName/SubTest
```

### Build CLI

To compile a local CLI binary to `./target`, run in the container:
```
make build-local
```

To compile the CLI binary for all architectures to `./target`, run in the container:
```
make build
```

### Start Documentation Server

To start the [Go Doc](https://go.dev/doc/) documentation server, run the command bellow, then open http://localhost:6060/pkg/github.com/keboola/keboola-as-code/?m=all:
```
make godoc
```

`m=all` is important to show also private packages.

### API Development

API development uses [Goa code generator](https://goa.design/).

To run the API locally:
1. Start `bash` in the dev container, run `docker compose run --rm -u "$UID:$GID" --service-ports dev bash`.
2. Set env var with Keboola stack: `export TEMPLATES_STORAGE_API_HOST=connection.keboola.com` (or `BUFFER_API_STORAGE_API_HOST`)
3. In the container run `make run-templates-api` (or `make run-buffer-api`)
 - The API is exposed to `http://localhost:8000/` (or `http://localhost:8001/`
 - When the code changes, the API recompiles and restarts.

To run Buffer Worker locally:
1. Start `bash` in the dev container, run `docker compose run --rm -u "$UID:$GID" --service-ports dev bash`.
2. Set env var with Keboola stack: `export BUFFER_WORKER_STORAGE_API_HOST=connection.keboola.com`
3. In the container run `make run-buffer-worker`

See [provisioning/stream/README.md](../provisioning/stream/README.md) or [provisioning/templates-api/README.md](../provisioning/templates-api/README.md) for more details about etcd and running in Minikube.


### E2E Tests

See [E2E_TESTS.md](./e2e_tests.md).

### IDE setup

The scripts `make mod`, `make fix`, `make ci` calls `go mod vendor`.
It syncs all dependencies (Go modules) to the `vendor` directory.
So integration with the IDE should work automatically.
