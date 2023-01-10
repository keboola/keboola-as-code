# Templates Architecture Overview

## CLI

Templates functionality is implemented also in the CLI. See the commands:
- [`local/template`](../../internal/pkg/service/cli/cmd/local/template) - Commands to apply and manage a template in the local folder.
- [`template`](../../internal/pkg/service/cli/cmd/template) - Commands to create a template in the project.
- [`template/repository`](../../internal/pkg/service/cli/cmd/template/repository) - Commands to manage a template repository folder.
- [`template/test`](../../internal/pkg/service/cli/cmd/template/test) - Commands to create and run template tests.

## API Entrypoint

- Entrypoint: [cmd/templates-api/main.go](../../cmd/templates-api/main.go)
- Starts HTTP server defined in: [internal/pkg/service/templates/api/http/http.go](../../internal/pkg/service/templates/api/http/http.go)
- The server utilizes code generated from the API design. 

## API Design

The API design can be found in [api/templates/design.go](../../api/templates/design.go) and is implemented using **[Goa framework](https://goa.design/)**.

There is `generate-templates-api` make command available to generate the code from the design specs. (The command is 
also run before other commands to run the API locally, build API image, release the API, etc.)

The command generates: 
- Code to [internal/pkg/service/templates/api/gen](../../internal/pkg/service/templates/api/gen).
- OpenAPI specifications to [internal/pkg/service/templates/api/openapi](../../internal/pkg/service/templates/api/openapi).

## Endpoints Implementation

Endpoints behavior is implemented in [internal/pkg/service/templates/api/service/service.go](../../internal/pkg/service/templates/api/service/service.go).

Endpoints code performs a validation of user inputs and typically runs one or more operations. (The operation
corresponds to the [`command design pattern`](https://refactoring.guru/design-patterns/command) but we decided to rename
the commands to operations not to confuse them with the CLI commands.)

The operations are defined in [`pkg/lib/operation`](../../pkg/lib/operation) folder. 
