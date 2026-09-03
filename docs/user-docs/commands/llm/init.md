# LLM Init Command

<div class="alert alert-info" role="alert">
<strong>BETA:</strong> The LLM commands are currently in beta. Features and output format may change.
</div>

**Initialize a new local directory for LLM export.**

```
kbc llm init [flags]
```

The command must be run in an empty directory.

This command creates the local manifest and metadata directory (`.keboola/`) without pulling any data from Keboola Connection.
Use [kbc llm export](export.md) after initialization to generate the AI-optimized project data.

If the command is run without options, it will start an interactive dialog asking for:
- URL of the [stack](https://help.keboola.com/overview/#stacks), for example, `connection.keboola.com`.
- [Storage API token](https://help.keboola.com/management/project/tokens/) to your project.
- Allowed [branches](https://help.keboola.com/tutorial/branches/) to work with.

## Options

`-H, --storage-api-host <string>`
: Keboola instance URL, e.g., "connection.keboola.com"

`-t, --storage-api-token <string>`
: Storage API token from your project

`-b, --branches <string>`
: Comma-separated list of branch IDs or name globs (use "*" for all)

`--allow-target-env`
: Allow usage of `KBC_PROJECT_ID` and `KBC_BRANCH_ID` environment variables for future operations

[Global Options](../../commands.md#global-options)

## Examples

```
➜ kbc llm init

Please enter the Keboola Storage API host, e.g., "connection.keboola.com".
? API host: connection.north-europe.azure.keboola.com

Please enter the Keboola Storage API token. Its value will be hidden.
? API token: ***************************************************

Please select which project's branches you want to use with this CLI.
? Allowed project's branches: only main branch

Created metadata directory ".keboola".
Created manifest file ".keboola/manifest.json".
Created file ".env.local" - it contains the API token, keep it local and secret.
Created file ".env.dist" - an ".env.local" template.
Created file ".gitignore" - to keep ".env.local" local.
```

## Next Steps

- [LLM Export](export.md)
- [All Commands](../../commands.md)
