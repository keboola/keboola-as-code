# Keboola as Code

A monorepo written in Go, represents [Keboola Connection](https://www.keboola.com/product/overview) project as definition files.

## Overview

### CLI

- Bidirectional synchronization of a local directory and [Keboola Connection](https://www.keboola.com/product/overview) project:
  - Push / pull operations.
  - Calls [Keboola Storage API](https://developers.keboola.com/integrate/storage/api/).
  - Local directory can be easly versioned by [Git](https://git-scm.com/) or a similar tool.
- Configurations are represented as JSON files.
- Transformations are represented as native files, for example SQL, Python, etc.
- Read more in the [Documentation](https://developers.keboola.com/cli/).
- CLI entrypoint: [cmd/kbc/main.go](https://github.com/keboola/keboola-as-code/blob/main/cmd/kbc/main.go)

### Templates

- To create a template from an existing [Keboola Connection](https://www.keboola.com/product/overview) project.
- To apply template to some other [Keboola Connection](https://www.keboola.com/product/overview) project.
- The [Jsonnet](https://jsonnet.org/) language is used to define the JSON files.
- In experimental phase, available via CLI.

## Development

- This project is primarily developed by [Keboola](https://www.keboola.com/).
- Suggestions for improvements and new features can be submitted at:  
  https://www.keboola.com/resources/roadmap.
- You can also send PR directly, but we do not guarantee that it will be accepted.
- See the [developer's guide](./docs/DEVELOPMENT.md).
