# Stream Architecture Overview

- The Stream Service consists of API and Worker nodes.
- API nodes contain configuration endpoints and collect data via the Import endpoint.
- Worker nodes import the collected data into Storage tables in batches.

User documentation can be found at [https://developers.keboola.com/integrate/push-data/](https://developers.keboola.com/integrate/push-data/).

## API Entrypoint

- Entrypoint: [cmd/stream/main.go](../../cmd/stream/main.go)
- The server utilizes code generated from the API design.

## API Design

The API design can be found in [api/stream/design.go](../../api/stream/design.go) and is implemented using **[Goa framework](https://goa.design/)**.

There is `generate-stream-api` task command available to generate the code from the design specs. (The command is
also run before other commands to run the API locally, build API image, release the API, etc.)

The command generates:
- Code to [internal/pkg/service/stream/api/gen](../../internal/pkg/service/stream/api/gen).
- OpenAPI specifications to [internal/pkg/service/stream/api/openapi](../../internal/pkg/service/stream/api/openapi).

## Endpoints Implementation

Endpoints behavior is implemented in [internal/pkg/service/stream/api/service/service.go](../../internal/pkg/service/stream/api/service/service.go).

Endpoints code performs validation of user inputs and typically runs one or more operations. 
See [internal/pkg/service/common/dependencies/dependencies.go](../../internal/pkg/service/common/dependencies/dependencies.go)
for a detailed explanation of dependency injection and the command design pattern implementation.

## Worker Implementation

TODO

## Resources

The Service uses an [etcd](https://etcd.io/) database to stream incoming data until they are imported to Storage.

## Other information

- [API Benchmarks](./benchmarks.md)
