# Development

This guide covers Docker-based development setup. For development directly on your local machine without Docker, see [Local Development Guide](local_development.md).

## Quick Start

### Clone

Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/keboola-as-code
cd keboola-as-code
docker compose build
```

It is possible when you do development on local environment and use `docker dev` image for `lint/build/test` there could be problem with `/vendor` file.

Call `task fix` or `go mod vendor` to fix the `vendor` packages. Also when rebasing and there are adjustements in `go.mod`.

### Setup ENV

Create `.env` file with definition of testing projects for example:
```
TEST_KBC_TMP_DIR=/tmp
TEST_KBC_PROJECTS_FILE=~/keboola-as-code/projects.json
```

The `~/your/path/to/keboola-as-code/projects.json` points to `gitignored` file that you are supposed to create within the project. The structure of this json file is according to this schema:
```
[{"host":"connection.keboola.com","project":1234,"stagingStorage":"s3","backend":"snowflake/bigquery","token":"<token>", "legacyTransformation": "false/true"},...]
```

Staging storage can be `s3`, `abs` or `gcs`, according to the stack. LegacyTransformations are support only on stacks different than `gcp`.

### Start Dev Container

Start an interactive console in a container, run:
```
docker compose run --rm -u "$UID:$GID" --service-ports dev bash
```

### Start Dev Container inspecting metrics

When we would like to inspect metrics of our services, there have to be made adjustement under `dev` service.
Use following command there, so the prometheus and dev service is connected
```
command: >
sh -c "git config --global --add safe.directory /code
       task run-<your-service>"
```

This ensures that the `/code` is safe directory to run. Make sure that under `<your-service>` you replace with desired service. E.g `task run-apps-proxy`.
Then run this docker compose command
```
docker compose up -d
```

### Inspecting prometheus

On `localhost:9090` the prometheus is running the UI where your service should be scraped. You can simply check them as graph visually.

### Troubleshooting

Strange issues are usually caused by incorrect permissions or file owner somewhere. Here are some tips:

In case you're using Docker Desktop on Linux remove the `-u "$UID:$GID"`. It can cause some permission issues.

Run `sudo chown -R <user>:<group> ./` to reset owner on all files.

Run `docker volume ls` and `docker volume rm <volume>` to delete the named cache volume.

When you want to use `replace github.com/your/module => /newpath` in `go.mod`, there has to be adjusted `docker-compose.yml`.

```yml
services:
  dev:
    # ...
  volumes:
    - ./:/code:z
    - cache:/tmp/cache
    - /path/in/your/system/module:/newpath
```

This breaks usually the editor as the replace cannot be found in `/newpath`. Therefore be carefull that this works only with docker image and on local you could face issues.

When in `.env` file there are multiple `TEST_KBC_PROJECTS` set up and you switch between the stacks, make sure that you rerun the

```
docker compose run --rm -u "$UID:$GID" --service-ports dev bash
```

as this command automatically fetches the `.env` file and sets up `TEST_KBC_PROJECTS` instead of you.

### Run Tests

Run the test suite and download the dependencies using:
```
task ci
```

To run only the CLI [E2E tests](./e2e_tests.md), you can use:
```
task tests-cli
```

To run a specific E2E test, you can use:
```
task e2e -- test/cli/path/to/test
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
task build-local
```

To compile the CLI binary for all architectures to `./target`, run in the container:
```
task build
```

### Start Documentation Server

To start the [Go Doc](https://go.dev/doc/) documentation server, run the command bellow, then open http://localhost:6060/pkg/github.com/keboola/keboola-as-code/?m=all:
```
task godoc
```

`m=all` is important to show also private packages.

### API Development

API development uses [Goa code generator](https://goa.design/).

To run the API locally:
1. Start `bash` in the dev container, run `docker compose run --rm -u "$UID:$GID" --service-ports dev bash`.
2. Set env var with Keboola stack: `export TEMPLATES_STORAGE_API_HOST=connection.keboola.com` (or `STREAM_STORAGE_API_HOST`)
3. In the container run `task run-templates-api` (or `task run-stream-service`)
 - The API is exposed to `http://localhost:8000/` (or `http://localhost:8001/`
 - When the code changes, the API recompiles and restarts.

To run Stream Worker locally:
1. Start `bash` in the dev container, run `docker compose run --rm -u "$UID:$GID" --service-ports dev bash`.
2. Set env var with Keboola stack: `export STREAM_WORKER_STORAGE_API_HOST=connection.keboola.com`
3. In the container run `task run-stream-worker`

**Note:** Stream service production deployment is now managed via GitOps. Only `provisioning/stream/dev` and `provisioning/stream/docker` remain for local development.


### Inspecting OpenAPI documentation

When we would like to inspect OpenAPI generated by **[Goa framework](https://goa.design/)**, simply go to **localhost:port/v1/documentation**. Where port is port of the service that the OpenAPI exists.

### E2E Tests

See [E2E_TESTS.md](./e2e_tests.md).

### IDE setup

The scripts `task mod`, `task fix`, `task ci` calls `go mod vendor`.
It syncs all dependencies (Go modules) to the `vendor` directory.
So integration with the IDE should work automatically.
