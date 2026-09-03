# Commands

Run `help` to list all available commands.
```
kbc help
```

You can also get details of any command.
```
kbc help <command>
kbc help local create row
```

## Available Commands

|---
| Command | Description
|-|-|-
| [kbc help](commands/help.md) | Show help for any command. |
| [kbc status](commands/status.md) | Show information about a working directory. |
| | |
| **[kbc sync](commands/sync.md)** | **Synchronization between a [local directory](structure.md) and a [project](README.md#subsystems).** |
| [kbc sync init](commands/sync/init.md) | Initialize a new local directory and run `kbc sync pull`. |
| [kbc sync pull](commands/sync/pull.md) | Sync a project to the local directory. |
| [kbc sync push](commands/sync/push.md) | Sync a local directory to the project. |
| [kbc sync diff](commands/sync/diff.md) | Show differences between a local directory and a project. |
| | |
| **[kbc ci](commands/ci.md)** | **Manage the CI/CD pipeline.** |
| [kbc ci workflows](commands/ci/workflows.md) | Generate workflows for [GitHub Actions integration](github-integration.md). |
| | |
| **[kbc local](commands/local.md)** | **Operations in the [local directory](structure.md) don't affect the project.** |
| [kbc local create](commands/local/create.md) | Create an object in the local directory. |
| [kbc local create config](commands/local/create/config.md) | Create an empty [configuration](https://help.keboola.com/components/). |
| [kbc local create row](commands/local/create/row.md) | Create an empty [configuration row](https://help.keboola.com/components/#configuration-rows). |
| [kbc local persist](commands/local/persist.md) | Detect new directories with a [configuration](https://help.keboola.com/components/) or a [configuration row](https://help.keboola.com/components/#configuration-rows). |
| [kbc local encrypt](commands/local/encrypt.md) | Encrypt all [unencrypted secrets](https://help.keboola.com/extend/common-interface/config-file/#encryption). |
| [kbc local validate](commands/local/validate.md) | Validate the local directory. |
| [kbc local validate config](commands/local/validate/config.md) | Validate a configuration JSON file. |
| [kbc local validate row](commands/local/validate/row.md) | Validate a configuration row JSON file. |
| [kbc local validate schema](commands/local/validate/schema.md) | Validate a configuration/row JSON file by a JSON schema file. |
| [kbc local fix-paths](commands/local/fix-paths.md) | Ensure that all local paths match [configured naming](structure.md#naming). |
| | |
| **[kbc remote](commands/remote.md)** | **Operations directly in the [project](README.md#subsystems).** |
| [kbc remote create](commands/remote/create.md) | Create an object in the project. |
| [kbc remote create branch](commands/remote/create/branch.md) | Create a new [branch](https://help.keboola.com/components/branches/) from the `main` branch. |
| [kbc remote create bucket](commands/remote/create/bucket.md) | Create a new [bucket](https://help.keboola.com/storage/buckets/). |
| [kbc remote file](commands/remote/file.md) | Manage [files](https://help.keboola.com/storage/files/) in Storage. |
| [kbc remote file download](commands/remote/file/download.md) | Download a [file](https://help.keboola.com/storage/files/) from Storage. |
| [kbc remote file upload](commands/remote/file/upload.md) | Upload a [file](https://help.keboola.com/storage/files/) to Storage. |
| [kbc remote job](commands/remote/job.md) | Manage [jobs](https://help.keboola.com/management/jobs/) in the project. |
| [kbc remote job run](commands/remote/job/run.md) | Run one or more [jobs](https://help.keboola.com/management/jobs/). |
| [kbc remote table](commands/remote/table.md) | Manage [tables](https://help.keboola.com/storage/tables/) in the project. |
| [kbc remote table create](commands/remote/table/create.md) | Create a new [table](https://help.keboola.com/storage/tables/). |
| [kbc remote table upload](commands/remote/table/upload.md) | Upload a CSV file to a [table](https://help.keboola.com/storage/tables/). |
| [kbc remote table download](commands/remote/table/download.md) | Download data from a [table](https://help.keboola.com/storage/tables/). |
| [kbc remote table preview](commands/remote/table/preview.md) | Preview up to 1000 rows from a [table](https://help.keboola.com/storage/tables/). |
| [kbc remote table detail](commands/remote/table/detail.md) | Print [table](https://help.keboola.com/storage/tables/) details. |
| [kbc remote table import](commands/remote/table/import.md) | Import data to a [table](https://help.keboola.com/storage/tables/) from a [file](https://help.keboola.com/storage/files/). |
| [kbc remote table unload](commands/remote/table/unload.md) | Unload a [table](https://help.keboola.com/storage/tables/) into a [file](https://help.keboola.com/storage/files/). |
| [kbc remote workspace](commands/remote/create.md) | Manage workspaces in the project. |
| [kbc remote workspace create](commands/remote/workspace/create.md) | Create a workspace in the project. |
| [kbc remote workspace delete](commands/remote/workspace/delete.md) | Delete a workspace in the project. |
| [kbc remote workspace detail](commands/remote/workspace/detail.md) | Print workspace details and credentials. |
| [kbc remote workspace list](commands/remote/workspace/list.md) | List workspaces in the project. |
| | |
| **[kbc dbt](commands/dbt.md)** | **Work with dbt inside your repository.** |
| [kbc dbt init](commands/dbt/init.md) | Initialize profiles, sources, and environment variables for use with dbt. |
| [kbc dbt generate](commands/dbt/generate.md) | Generate profiles, sources, and environment variables for use with dbt. |
| [kbc dbt generate profile](commands/dbt/generate/profile.md) | Generate profiles for use with dbt. |
| [kbc dbt generate sources](commands/dbt/generate/sources.md) | Generate sources for use with dbt. |
| [kbc dbt generate env](commands/dbt/generate/env.md) | Generate environment variables for use with dbt. |
| | |
| **[kbc llm](commands/llm.md) (BETA)** | **Export project data to AI-optimized format.** |
| [kbc llm init](commands/llm/init.md) | Initialize a new local directory for LLM export. |
| [kbc llm export](commands/llm/export.md) | Export project data to AI-optimized twin format. |

## Aliases

The most used commands have their shorter aliases.

For example, you can use `kbc c` instead of `kbc local create`.

|---
| Full Command | Aliases
|-|-|-
| `kbc sync init`      |  `kbc init`, `kbc i`
| `kbc sync diff`      |  `kbc diff`, `kbc d`
| `kbc sync pull`      |  `kbc pull`, `kbc pl` 
| `kbc sync push`      |  `kbc push`, `kbc ph`
| `kbc local validate` |  `kbc validate`, `kbc v`
| `kbc local persist`  |  `kbc persist`, `kbc pt`
| `kbc local create`   |  `kbc create`, `kbc c`
| `kbc local encrypt`  |  `kbc encrypt`, `kbc e`

## Options 

Options are a way to modify the behavior of a command, they can be:
- **[Global](#global-options)**, for all commands, see below.
- **Local**, only for a specific command, see the command help.

#### Command-line flags

- Entered as part of the CLI command.
- One-letter flags start with `-`, for example `-v`.
- Longer flags start with `--`, for example `--verbose`.
- **Flags take precedence over environment variables.**


#### Environment variables

- Each flag can be defined via an environment variable.
- Variable name is based on the flag name, and starts with `KBC_`.
- All letters are changed to uppercase and dashes to underscores.
- For example, flag `--log-file` can be defined by the `KBC_LOG_FILE` environment variable.
- Sources and priority of the environment variables:
    1. From the OS environment.
    2. From environment files in the working directory.
    3. From environment files in the project directory.

All found environment files are automatically loaded.  
Variables are merged together according to the following priority.

|---
| Environment File | Environment | Priority
|-|-|-
| `.env.development.local`  | Development | The highest |  
| `.env.test.local`         | Test |  |
| `.env.production.local`   | Production |  |
| `.env.local`              | Wherever the file is |  |
| `.env.development`        | Development|  |
| `.env.test`               | Test|  |
| `.env.production`         | Production|  |
| `.env`                    | All | The lowest  |

*Note: All `.*local` environment files should be part of the `.gitignore` file, if used.*

### Global Options

`-h, --help`
: Show help for the command

`-l, --log-file <string>`
: Path to a log file to store the details in

`-t, --storage-api-token <string>`
: Storage API token to the project

`-v, --verbose`
: Increase output verbosity

`--verbose-api`
: Log each API request and its response

`-V, --version`
: Show the version

`-d, --working-dir <string>`
: Use another working directory

## Next Steps

- [Installation](installation.md)
- [Getting Started](getting-started.md)
- [Directory Structure](structure.md)
- [GitHub Integration](github-integration.md)
