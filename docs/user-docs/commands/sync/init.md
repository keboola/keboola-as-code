# Init Command

**Initialize a new [local directory](../../structure.md) and run the first [pull](pull.md).**

```
kbc sync init [flags]
```

Or shorter:
```
kbc init [flags]
kbc i [flags]
```

The command must be run in an empty directory.

If the command is run without options, it will start an interactive dialog asking for:
- URL of the [stack](https://help.keboola.com/overview/#stacks), for example, `connection.keboola.com`.
- [Master token](https://help.keboola.com/management/project/tokens/#master-tokens) to your project.
- Allowed [branches](https://help.keboola.com/tutorial/branches/) to work with.

It will allow you to create GitHub Actions workflows in the directory.

## Options

`--allow-target-env`
: Allow usage of `KBC_PROJECT_ID` and `KBC_BRANCH_ID` environment variables for future operations.  
Sets `true` to the `allowTargetEnv` field in the [manifest.json](../../structure.md#manifest).

`-b, --branches <string>`
: Comma-separated list of branch IDs or name globs (use "*" for all) for branches you want to work with locally (default "main"); other branches in the project will be ignored.

`--ci-main-branch <string>`
: Name of the main branch for push/pull workflows (default "main")

`--ci-pull <bool>`
: Create a workflow to sync the main branch from the project every hour (default true)

`--ci-push <bool>`
: Create a workflow to push changes in the main branch to the project (default true)

`--ci-validate <bool>`
: Create a workflow to validate all branches on change (default true)

`--skip-workflows`
: Skip the interactive GitHub workflow setup

`-H, --storage-api-host <string>` 
: Keboola instance URL, e.g., "connection.keboola.com"

[Global Options](../../commands.md#global-options)

## Examples

```
➜ kbc init

Please enter the Keboola Storage API host, e.g., "connection.keboola.com".
? API host connection.north-europe.azure.keboola.com

Please enter the Keboola Storage API token. Its value will be hidden.
? API token ***************************************************

Please select which project's branches you want to use with this CLI.
The other branches will still exist, but they will be invisible in the CLI.
? Allowed project's branches: only main branch

Created metadata directory ".keboola".
Created manifest file ".keboola/manifest.json".
Created file ".env.local" - it contains the API token, keep it local and secret.
Created file ".env.dist" - an ".env.local" template.
Created file ".gitignore" - to keep ".env.local" local.

? Generate workflows files for GitHub Actions? No

Init done. Running pull.
Plan for "pull" operation:
+ B main
+ C main/extractor/ex-generic-v2/empty
+ C main/extractor/keboola.ex-aws-s3/my-aws-s-3-data-source
+ R main/extractor/keboola.ex-aws-s3/my-aws-s-3-data-source/rows/share-cities
+ C main/extractor/keboola.ex-google-drive/my-google-drive-data-source
+ C main/extractor/keboola.ex-google-drive/my-google-drive-data-source/schedules/scheduler-for-7243915
+ C main/other/keboola.orchestrator/daily
+ C main/other/keboola.orchestrator/daily/schedules/scheduler-for-7243915
+ C main/other/keboola.sandboxes/address
+ C main/transformation/keboola.snowflake-transformation/address
+ C main/transformation/keboola.snowflake-transformation/address/variables
+ R main/transformation/keboola.snowflake-transformation/address/variables/values/default
Pull done.
```

## Next Steps

- [All Commands](../../commands.md)
- [Pull](pull.md)
- [Push](push.md)
- [Diff](diff.md)
