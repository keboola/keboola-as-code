# Templates Architecture Overview

- CLI commands and API to create a template from an existing [Keboola](https://www.keboola.com/product/overview) project and to apply a template to some other [Keboola Connection](https://www.keboola.com/product/overview) project.
- The [Jsonnet](https://jsonnet.org/) language is used to define JSON configurations in the templates.

The [user documentation](https://developers.keboola.com/cli/templates/) describes how to create and use the templates.

## CLI

Templates functionality is implemented also in the CLI. See the commands:
- [`local/template`](../../internal/pkg/service/cli/cmd/local/template) - Commands to apply and manage a template in the local folder.
- [`template`](../../internal/pkg/service/cli/cmd/template) - Commands to create a template in the project.
- [`template/repository`](../../internal/pkg/service/cli/cmd/template/repository) - Commands to manage a template repository folder.
- [`template/test`](../../internal/pkg/service/cli/cmd/template/test) - Commands to create and run template tests.

## API Entrypoint

- Entrypoint: [cmd/templates-api/main.go](../../cmd/templates-api/main.go)
- The server utilizes code generated from the API design. 

## API Design

The API design can be found in [api/templates/design.go](../../api/templates/design.go) and is implemented using **[Goa framework](https://goa.design/)**.

There is `generate-templates-api` task command available to generate the code from the design specs. (The command is
also run before other commands to run the API locally, build API image, release the API, etc.)

The command generates: 
- Code to [internal/pkg/service/templates/api/gen](../../internal/pkg/service/templates/api/gen).
- OpenAPI specifications to [internal/pkg/service/templates/api/openapi](../../internal/pkg/service/templates/api/openapi).

## Endpoints Implementation

Endpoints behavior is implemented in [internal/pkg/service/templates/api/service/service.go](../../internal/pkg/service/templates/api/service/service.go).

Endpoints code performs validation of user inputs and typically runs one or more operations.
See [internal/pkg/service/common/dependencies/dependencies.go](../../internal/pkg/service/common/dependencies/dependencies.go)
for a detailed explanation of dependency injection and the command design pattern implementation.

## Resources

The Service uses an [etcd](https://etcd.io/) database for shared locks mechanism to ensure atomicity of write operations 
through the API. etcd availability is not critical as the API can work without it but then it does not ensure the atomicity.
