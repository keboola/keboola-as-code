# Keboola Go Monorepo

A monorepo written in Go. Contains

- CLI for representing [Keboola Connection](https://www.keboola.com/product/overview) project as definition files.
- API for applying templates to Keboola projects.
- API for continuous sending of small data amounts and their batch import to Storage tables.

## Overview

### Keboola as Code 

#### CLI

- Bidirectional synchronization of a local directory and [Keboola Connection](https://www.keboola.com/product/overview) project:
  - Push / pull operations.
  - Calls [Keboola Storage API](https://developers.keboola.com/integrate/storage/api/).
  - Local directory can be easily versioned by [Git](https://git-scm.com/) or a similar tool.
- Configurations are represented as JSON files.
- Transformations are represented as native files, for example SQL, Python, etc.
- Read more in the [Documentation](https://developers.keboola.com/cli/).
- See [architecture overview](./docs/cli/overview.md)

#### Templates Service

- To create a template from an existing [Keboola Connection](https://www.keboola.com/product/overview) project.
- To apply a template to some other [Keboola Connection](https://www.keboola.com/product/overview) project.
- The [Jsonnet](https://jsonnet.org/) language is used to define the JSON files.
- Available via CLI and API.
- See [architecture overview](./docs/templates/overview.md)

### Buffer Service

- A Proxy API to import data to Storage tables.
- Incoming data are buffered until some configured condition is met to import them to Storage in a single batch.
- See [architecture overview](./docs/buffer/overview.md)


## Development

- This project is primarily developed by [Keboola](https://www.keboola.com/).
- Suggestions for improvements and new features can be submitted at:  
  https://www.keboola.com/resources/roadmap.
- You can also send PR directly, but we do not guarantee that it will be accepted.
- See the [developer's guide](./docs/DEVELOPMENT.md) and [description of the release process](./docs/RELEASE.md).

## License

MIT licensed, see [LICENSE](./LICENSE) file.
