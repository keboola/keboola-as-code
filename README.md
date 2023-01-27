# Keboola Go Monorepo

## Overview

### Keboola as Code 

#### CLI

- Provides a representation of [Keboola](https://www.keboola.com/product/overview) project in a local directory and its bidirectional synchronization.
- Supports direct operations on the remote project.
- See [user documentation](https://developers.keboola.com/cli/).
- See **[architecture overview](./docs/cli/overview.md)** for more details.

#### Templates Service

- Provides CLI commands and API for creating templates from existing [Keboola](https://www.keboola.com/product/overview) project and their usage in other projects.
- See [user documentation](https://developers.keboola.com/cli/templates/).
- See **[architecture overview](./docs/templates/overview.md)** for more details.

### Buffer Service

- A Proxy API to buffer collected data and their import to Storage tables in batches.
- See **[architecture overview](./docs/buffer/overview.md)** for more details.


## Development

- This project is primarily developed by [Keboola](https://www.keboola.com/).
- Suggestions for improvements and new features can be submitted at:  
  https://www.keboola.com/resources/roadmap.
- You can also send PR directly, but we do not guarantee that it will be accepted.

## Other Links 
- [Developers' guide](./docs/DEVELOPMENT.md)
- [Release process](./docs/RELEASE.md)
- [E2E tests](./docs/e2etests.md)

## License

MIT licensed, see [LICENSE](./LICENSE) file.
