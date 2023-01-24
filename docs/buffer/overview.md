# Buffer Architecture Overview

- The Buffer Service consists of API and Worker nodes.
- API nodes contain configuration endpoints and collect data via the Import endpoint.
- Worker nodes import the collected data into Storage tables in batches.

User documentation can be found at [https://developers.keboola.com/integrate/push-data/](https://developers.keboola.com/integrate/push-data/).

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
Entrypoint: [cmd/buffer-worker/main.go](../../cmd/buffer-worker/main.go).

Worker behavior is implemented in [internal/pkg/service/buffer/worker/service/service.go](../../internal/pkg/service/buffer/worker/service/service.go).

The [`internal/pkg/service/buffer/watcher`](../../internal/pkg/service/buffer/watcher) package provides cache for API nodes
and synchronization between API/Worker nodes. See [internal/pkg/service/buffer/watcher/watcher.go](../../internal/pkg/service/buffer/watcher/watcher.go) for details.

The [`internal/pkg/service/buffer/worker/distribution`](../../internal/pkg/service/buffer/worker/distribution) package
provides distribution of various keys/tasks between worker nodes. See [internal/pkg/service/buffer/worker/distribution/doc.go](../../internal/pkg/service/buffer/worker/distribution/doc.go) for details.

The [`internal/pkg/service/buffer/worker/task`](../../internal/pkg/service/buffer/worker/task) package
provides task abstraction for long-running operations in the Worker node.

The [`internal/pkg/service/buffer/worker/task/orchestrator`](../../internal/pkg/service/buffer/worker/task/orchestrator) package
combines `distribution.Node` and `task.Node` to run a task on one node in the cluster only, as a reaction to a watch event. See [internal/pkg/service/buffer/worker/task/orchestrator/orchestrator.go](../../internal/pkg/service/buffer/worker/task/orchestrator/orchestrator.go) for details.

## Resources

The Service uses an [etcd](https://etcd.io/) database to buffer incoming data until they are imported to Storage.

## Other information

- [API Benchmarks](./benchmarks.md)
