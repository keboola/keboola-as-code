# Buffer Architecture Overview

- A Proxy API to buffer collected data and their import to Storage tables in batches.

**TODO Link to user docs**

## API Entrypoint

- Entrypoint: [cmd/buffer-api/main.go](../../cmd/buffer-api/main.go)
- Starts HTTP server defined in: [internal/pkg/service/buffer/api/http/http.go](../../internal/pkg/service/buffer/api/http/http.go)
- The server utilizes code generated from the API design.

## API Design

The API design can be found in [api/buffer/design.go](../../api/buffer/design.go) and is implemented using **[Goa framework](https://goa.design/)**.

There is `generate-buffer-api` make command available to generate the code from the design specs. (The command is
also run before other commands to run the API locally, build API image, release the API, etc.)

The command generates:
- Code to [internal/pkg/service/buffer/api/gen](../../internal/pkg/service/buffer/api/gen).
- OpenAPI specifications to [internal/pkg/service/buffer/api/openapi](../../internal/pkg/service/buffer/api/openapi).

## Endpoints Implementation

Endpoints behavior is implemented in [internal/pkg/service/buffer/api/service/service.go](../../internal/pkg/service/buffer/api/service/service.go).

Endpoints code performs validation of user inputs and typically runs one or more operations. 
See [internal/pkg/service/common/dependencies/dependencies.go](../../internal/pkg/service/common/dependencies/dependencies.go)
for a detailed explanation of dependency injection and the command design pattern implementation.

## Worker Implementation

- Entrypoint: [cmd/buffer-worker/main.go](../../cmd/buffer-worker/main.go)

**TODO**

## Resources

The Service uses an [etcd](https://etcd.io/) database to buffer incoming data until they are imported to Storage.

## Other information

- [API Benchmarks](./benchmarks.md)
