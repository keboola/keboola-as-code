# Delete Workspace

**Delete a [workspace](https://help.keboola.com/transformations/workspace/).**

```
kbc remote workspace delete [flags]
```

### Options

`-W, --workspace-id string`
: ID of the workspace to be deleted. You can find it using the [List Workspaces](list.md) command.

`-H, --storage-api-host <string>` 
: Keboola instance URL, e.g., "connection.keboola.com"

[Global Options](../../../commands.md#global-options)

### Examples

```
➜ kbc remote workspace delete -W <id>

Deleting the workspace "foo" (<id>), please wait.
Delete done.
```

## Next Steps

- [All Commands](../../../commands.md)
- [Learn more about Workspaces](https://help.keboola.com/transformations/workspace/)
