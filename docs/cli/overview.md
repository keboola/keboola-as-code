# CLI Architecture Overview

- Bidirectional synchronization of a local directory and [Keboola Connection](https://www.keboola.com/product/overview) project:
    - Push / pull operations.
    - Calls [Keboola Storage API](https://developers.keboola.com/integrate/storage/api/).
    - Local directory can be easily versioned by [Git](https://git-scm.com/) or a similar tool.
- Configurations in the local directory are represented as JSON files, transformations are represented as native files (SQL, Python, etc).

The installation, usage, and complete list of available commands are available at [https://developers.keboola.com/cli/](https://developers.keboola.com/cli/). 

## Entrypoint

[cmd/kbc/main.go](../../cmd/kbc/main.go)

## CLI Commands

Use [Cobra framework](https://cobra.dev/).

CLI commands are defined in [`internal/pkg/service/cli/cmd/`](../../internal/pkg/service/cli/cmd):

- [`ci`](../../internal/pkg/service/cli/cmd/ci) - Commands to generate CI workflows for the local project.
- [`dbt`](../../internal/pkg/service/cli/cmd/dbt) - Commands to generate definitions for dbt.
- [`local`](../../internal/pkg/service/cli/cmd/local) - Commands to work locally inside the project folder (i.e. create a config or apply a template).
- [`remote`](../../internal/pkg/service/cli/cmd/remote) - Commands to work remotely in the Keboola project (i.e. create a branch, create a workspace).
- [`sync`](../../internal/pkg/service/cli/cmd/sync) - Commands to synchronize state between the local folder and its Keboola project (i.e. pull changes from the project, push changes to the project).
- [`template`](../../internal/pkg/service/cli/cmd/template) - Commands to manage templates in the project or manage the templates repository.

## Dialogs

Most CLI commands offer an interactive dialog to fill in the needed information. E.g. `local create` command asks what to create 
by calling [`d.Dialogs().AskWhatCreateLocal()`](https://github.com/keboola/keboola-as-code/blob/26baba1315236ed6b9cc810892ac440bf7da9d7e/internal/pkg/service/cli/cmd/local/create/cmd.go#L42-L49).
The user inputs can be filled in a non-interactive mode using command flags.

The dialogs are defined in the [`internal/pkg/service/cli/dialog`](../../internal/pkg/service/cli/dialog) package.

They use [`internal/pkg/service/cli/prompt`](../../internal/pkg/service/cli/prompt) package that defines `Prompt` interface for the following types of user inputs:

- `Confirm` - Expects `Yes` or `No` answer.
- `Question` - Expects a typed answer.
- `Select` - Lists options to choose one from.
- `SelectIndex` - Lists options indexed by a number. Returns index for the selected option.
- `MultiSelect` - Lists options and allows choosing any number of them.
- `MultiSelectIndex` - Lists options indexed by a number and allows choosing any number of them. Returns an array of indices for the selected options.
- `Multiline` - Allows a multi-line answer.

## Operations

After collecting user inputs, a CLI command typically calls an operation or a set of operations. (The operation 
corresponds to the [`command design pattern`](https://refactoring.guru/design-patterns/command) but we decided to rename
the commands to operations not to confuse them with the CLI commands.)

The operations are defined in [`pkg/lib/operation`](../../pkg/lib/operation) folder. 

See [internal/pkg/service/common/dependencies/dependencies.go](../../internal/pkg/service/common/dependencies/dependencies.go)
for a detailed explanation of dependency injection and the command design pattern implementation. 
