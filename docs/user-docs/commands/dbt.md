# dbt Command

**Work with dbt inside your repository.**

The commands must be run in a directory with a dbt project (i.e. containing `dbt_project.yml`) or its subdirectory.

See the [introduction to dbt support](../dbt.md) for more information.

```
kbc dbt [command]
```

|---
| Command | Description
|-|-|-
| [kbc dbt init](dbt/init.md) | Initialize profiles, sources, and environment variables for use with dbt. |
| [kbc dbt generate](dbt/generate.md) | Generate profiles, sources, or environment variables for use with dbt. |
| [kbc dbt generate profile](dbt/generate/profile.md) | Generate profiles for use with dbt. |
| [kbc dbt generate sources](dbt/generate/sources.md) | Generate sources for use with dbt. |
| [kbc dbt generate env](dbt/generate/env.md) | Generate environment variables for use with dbt. |
