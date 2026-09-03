# Generate Env Command

**Generates environment variables for use with dbt.**

```
kbc dbt generate env [flags]
```

The command must be run in a directory with a dbt project (i.e., containing `dbt_project.yml`) or its subdirectory.

The command outputs commands to create environment variables from a selected existing Snowflake workspace.

See the [introduction to dbt support](../../../dbt.md) for more information.

## Options

`-H, --storage-api-host <string>`
: Storage API host, e.g., "connection.keboola.com"

`-T, --target-name <string>`
: Target name of the profile

`-W, --workspace-id <string>`
: ID of the workspace to use

[Global Options](../../../commands.md#global-options)

## Examples

```
➜ kbc dbt generate env

Please enter the Keboola Storage API host, e.g., "connection.keboola.com".
? API host: connection.north-europe.azure.keboola.com


Please enter the Keboola Storage API token. The value will be hidden.
? API token: **************************************************


Please enter the target name.
Allowed characters: a-z, A-Z, 0-9, "_".
? Target Name: target1


? Workspace: dbt_workspace (12345678)

Commands to set the environment for the dbt target:
  export DBT_KBC_TARGET1_TYPE=snowflake
  export DBT_KBC_TARGET1_SCHEMA=WORKSPACE_123456
  export DBT_KBC_TARGET1_WAREHOUSE=KEBOOLA_PROD_SMALL
  export DBT_KBC_TARGET1_DATABASE=KEBOOLA_1234
  export DBT_KBC_TARGET1_ACCOUNT=keboola.west-europe.azure
  export DBT_KBC_TARGET1_USER=KEBOOLA_WORKSPACE_123456
  export DBT_KBC_TARGET1_PASSWORD=abcd123456
```

## Next Steps

- [dbt generate](../generate.md)
- [Introduction to dbt support](../../../dbt.md)
