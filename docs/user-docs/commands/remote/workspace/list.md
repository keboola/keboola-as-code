# List Workspaces

**Print a list of [workspaces](https://help.keboola.com/transformations/workspace/).**

```
kbc remote workspace list [flags]
```

### Options

`-H, --storage-api-host <string>`
: Keboola instance URL, e.g., "connection.keboola.com"

[Global Options](../../../commands.md#global-options)

### Examples

```
➜ kbc remote workspace list

Loading workspaces, please wait.
Found workspaces:
  foo (ID: <id>, Type: snowflake)
  bar (ID: <id>, Type: snowflake)
  baz (ID: <id>, Type: python, Size: small)
```

## Next Steps

- [All Commands](../../../commands.md)
- [Learn more about Workspaces](https://help.keboola.com/transformations/workspace/)
