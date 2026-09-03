# CLI

Keboola CLI (Command Line Interface), known also as "Keboola as Code", is a set of commands for operating your cloud data 
pipeline. It is available to install in the Windows, macOS, and Linux environments.

The whole Keboola project is represented by a local [directory structure](structure.md#directory-structure). 
[Component configurations](https://help.keboola.com/components) are represented by [JSON files](structure.md#configurations).

## Use Cases

Keboola CLI can be used, for example, to:
- Pull your entire project to a local directory in seconds. See the [init](commands/sync/init.md) and [pull](commands/sync/pull.md) commands.
- Bulk edit [component configurations](https://help.keboola.com/components) in your IDE.
- Compare the local version with the current project state. See the [diff](commands/sync/diff.md) command.
- Copy a [configuration](https://help.keboola.com/components) as a directory in the project and between projects. See the [persist](commands/local/persist.md) command.
- Apply all changes back to the project in a moment. See the [push](commands/sync/push.md) command.
- Manage project history in a git repository.
- Automate the whole process in a CI/CD pipeline. See [GitHub Integration](github-integration.md). Use the `--skip-workflows` flag during initialization to avoid interactive prompts in automated environments.
- Merge and rebase Keboola Branches via Git. Learn more in the [Example Use Cases](devops-use-cases.md) section.
- Distribute a single project definition into multiple projects. See the [Example Use Cases](devops-use-cases.md) section.
- Multi-stage (and multi-project) environment management via Git. See the [Example Use Cases](devops-use-cases.md) section. 
- Locally develop and test your dbt transformation code.

## Subsystems

A brief overview of supported subsystems of the project.

### Configurations

- [Component configurations](https://help.keboola.com/components) and [configuration rows](https://help.keboola.com/components/#configuration-rows) are fully supported.
- This includes all special types of components, such as:
  - [Transformations](structure.md#transformations), [Variables](structure.md#variables), [Shared Codes](structure.md#shared-code), [Schedules](structure.md#schedules) and [Orchestrations](structure.md#orchestrations).   

### Development Branches

- A [branch](https://help.keboola.com/components/branches/)  can be [pulled](commands/sync/pull.md) and then edited or deleted locally. 
- Changes can be [pushed](commands/sync/push.md) back to the project.
- There is one limitation, **a branch cannot be created locally**. 
  - A branch must be created directly in the project, from the `main` branch.
  - See the [Create Branch](commands/remote/create/branch.md) command.

### Storage

At the moment, all [Storage](https://help.keboola.com/storage/) related operations are sub-commands of the [kbc remote](commands/remote.md) command. They operate directly on a project. This means that any changes you make using the CLI are immediately applied to your project. We have plans to add support for managing buckets and tables locally using definition files just like component configurations.


#### Files

- To upload a file, use the [file upload](commands/remote/file/upload.md) command.
- To download a file, use the [file download](commands/remote/file/download.md) command.

#### Buckets and tables

These commands can be used to manage the [buckets](https://help.keboola.com/storage/buckets/) and [tables](https://help.keboola.com/storage/tables/) in your project:
- To create a new bucket, use the [create bucket](commands/remote/create/bucket.md) command. 
- To create a new table, use the [create table](commands/remote/table/create.md) command.

The resulting [tables](https://help.keboola.com/storage/tables/) will be empty, so you may want to use:
- The [table import](commands/remote/table/import.md) command to import data. 
- The [table unload](commands/remote/table/unload.md) command can be used to take data out of a table and store it in a file.

For convenience, you can use combined commands:
- The [table upload](commands/remote/table/upload.md) command combines the [file upload](commands/remote/file/upload.md) + [table import](commands/remote/table/import.md) operations.
- The [table download](commands/remote/table/download.md) command combines the [table unload](commands/remote/table/unload.md) + [file download](commands/remote/file/download.md) operations.

These commands may be a little heavy if you are dealing with a lot of data.
- If you just want a quick sample, use the [table preview](commands/remote/table/preview.md) command.

## Next Steps

- [Installation](installation.md)
- [Getting Started](getting-started.md)
- [Directory Structure](structure.md)
- [Commands](commands.md)
