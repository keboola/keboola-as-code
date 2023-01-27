# Keboola as Code - Development

## Setup

Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/keboola-as-code
cd keboola-as-code
docker-compose build
```

Create `.env` file with definition of testing projects:
```
TEST_KBC_PROJECTS="[{"host":"connection.keboola.com","project":1234,"stagingStorage":"s3","token":"<token>"},...]"
```

Staging storage can be `s3` or `abs`, according to the stack.

Run the test suite and download the dependencies using:

```
docker-compose run --rm -u "$UID:$GID" dev make ci
```

To start the interactive console in the container, you can use:
```
docker-compose run --rm -u "$UID:$GID" --service-ports dev bash
```

To run the one or more tests via wildcard:
```
./scripts/tests.sh -run "TestCliE2E/pull/.*"
```

To run tests in one specific package:
```
go test ./internal/pkg/PACKAGE/...
```

To run tests with verbose output to see HTTP requests, ENVs, etc.:
```
docker-compose run --rm dev make tests-verbose
```

In the container, you can run CLI from the source code using:
```
go run ./src/main.go help init
```

To compile a local binary to `./target`, run in the container:
```
make build-local
```

To compile a binary for all architectures to `./target`, run in the container:
```
make build
```

### API Development

API development uses [Goa code generator](https://goa.design/).

To run the API locally:
1. Start `bash` in the dev container, run `docker-compose run --rm -u "$UID:$GID" --service-ports dev bash`.
2. In the container run `make run-templates-api`
 - The API is exposed to `http://localhost:8000/`.
 - When the code changes, the API recompiles and restarts.

### E2E Tests

See [e2etests.md](./e2etests.md).

### IDE setup

The scripts `make mod`, `make fix`, `make ci` calls `go mod vendor`.
It syncs all dependencies (Go modules) to the `vendor` directory.
So integration with the IDE should work automatically.
