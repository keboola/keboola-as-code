# dbt

Keboola CLI allows you to integrate with your dbt project. The commands must be run in a directory with a dbt project 
(i.e., containing `dbt_project.yml`) or its subdirectory.

[kbc dbt init](commands/dbt/init.md) command creates a Snowflake [workspace](https://help.keboola.com/transformations/workspace/)
in Keboola, configures a dbt target with it, generates source files for every table in Keboola Storage and outputs
commands to create environmental variables so that you don't store Snowflake credentials directly in the dbt configuration files. 

The command output will look like this:

```
➜ kbc dbt init

Please enter the Keboola Storage API host, e.g., "connection.keboola.com".
? API host: connection.north-europe.azure.keboola.com


Please enter the Keboola Storage API token. The value will be hidden.
? API token: **************************************************


Please enter the target name.
Allowed characters: a-z, A-Z, 0-9, "_".
? Target Name: target1


? Enter a name for the workspace to be created: dbt_workspace

Creating a new workspace, please wait.
Created the new workspace "dbt_workspace".
Profile stored in "profiles.yml".
Sources stored in the "models/_sources" directory.
Commands to set the environment for the dbt target:
  export DBT_KBC_TARGET1_TYPE=snowflake
  export DBT_KBC_TARGET1_SCHEMA=WORKSPACE_12345
  export DBT_KBC_TARGET1_WAREHOUSE=KEBOOLA_PROD_SMALL
  export DBT_KBC_TARGET1_DATABASE=KEBOOLA_1234
  export DBT_KBC_TARGET1_ACCOUNT=keboola.west-europe.azure
  export DBT_KBC_TARGET1_USER=KEBOOLA_WORKSPACE_12345
  export DBT_KBC_TARGET1_PASSWORD=abcd1234
```

## Profile

The target name is used for the configuration in the dbt's `profiles.yml` file. See the official documentation for more information: [https://docs.getdbt.com/reference/profiles.yml](https://docs.getdbt.com/reference/profiles.yml).

The created target in `profiles.yml` does not contain any sensitive information, it just references environmental variables. 


```yaml
TestProject:
    target: target1
    outputs:
        target1:
            account: '{{ env_var("DBT_KBC_TARGET1_ACCOUNT") }}'
            database: '{{ env_var("DBT_KBC_TARGET1_DATABASE") }}'
            password: '{{ env_var("DBT_KBC_TARGET1_PASSWORD") }}'
            schema: '{{ env_var("DBT_KBC_TARGET1_SCHEMA") }}'
            type: '{{ env_var("DBT_KBC_TARGET1_TYPE") }}'
            user: '{{ env_var("DBT_KBC_TARGET1_USER") }}'
            warehouse: '{{ env_var("DBT_KBC_TARGET1_WAREHOUSE") }}'
send_anonymous_usage_stats: false
```


## Sources

The sources are stored in the `models/_sources` directory and the command generates a separate file for every bucket. The directory
can look like this:

```
➜ ls -1 models/_sources

in.c-keboola-ex-facebook-8103426.yml
in.c-keboola-ex-facebook-ads-15044494.yml
in.c-keboola-ex-gcalendar-1279777.yml
in.c-test.yml
in.c-test1647518938917259000.yml
```

Each source file contains the definition for all tables in the specific bucket, like this:


```yaml
version: 2
sources:
    - name: in.c-test
      freshness:
        warn_after:
            count: 1
            period: day
      database: '{{ env_var("DBT_KBC_TARGET1_DATABASE") }}'
      schema: in.c-test
      loaded_at_field: '"_timestamp"'
      tables:
        - name: products
          quoting:
            database: true
            schema: true
            identifier: true
          columns: []
```


See the official documentation for more information: [https://docs.getdbt.com/docs/build/sources](https://docs.getdbt.com/docs/build/sources).

## Env

The command in the end outputs commands for setting all environment variables you are going to need for the dbt project.

```
  export DBT_KBC_TARGET1_TYPE=snowflake
  export DBT_KBC_TARGET1_SCHEMA=WORKSPACE_12345
  export DBT_KBC_TARGET1_WAREHOUSE=KEBOOLA_PROD_SMALL
  export DBT_KBC_TARGET1_DATABASE=KEBOOLA_1234
  export DBT_KBC_TARGET1_ACCOUNT=keboola.west-europe.azure
  export DBT_KBC_TARGET1_USER=KEBOOLA_WORKSPACE_12345
  export DBT_KBC_TARGET1_PASSWORD=abcd1234
  ```

Single steps in this command can be run separately; see [kbc dbt generate](commands/dbt/generate.md).

## Available Commands

|---
| Command | Description
|-|-|-
| [kbc dbt init](commands/dbt/init.md) | Initialize profiles, sources, and environment variables for use with dbt. |
| [kbc dbt generate](commands/dbt/generate.md) | Generate profiles, sources, or environment variables for use with dbt. |
| [kbc dbt generate profile](commands/dbt/generate/profile.md) | Generate profiles for use with dbt. |
| [kbc dbt generate sources](commands/dbt/generate/sources.md) | Generate sources for use with dbt. |
| [kbc dbt generate env](commands/dbt/generate/env.md) | Generate environment variables for use with dbt. |
