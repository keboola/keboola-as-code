# Pull Command

**Sync a [project](../../README.md#subsystems) to the [local directory](../../structure.md).**

```
kbc sync pull [flags]
```

Or shorter:
```
kbc pull [flags]
kbc pl [flags]
```

Local changes will be overwritten to match the state of the project. 

If your local state is invalid, the command will fail unless you use the `--force` flag.

## Options

`--dry-run`
: Preview all changes

`--force`
: Ignore invalid local state

`--cleanup-rename-conflicts`
: Enable cleanup mode for handling rename conflicts. When configurations are renamed in the UI (e.g., in a chain like A→B, C→A), this option removes conflicting destinations to allow the rename to proceed. **Only use this for UI-only workflows** where changes are made exclusively in the Keboola UI and synced down. This option is not needed for normal Git-based development workflows.

<div class="clearfix"></div><div class="alert alert-warning" role="alert" markdown="1">
<strong>Warning:</strong><br>
When using `--cleanup-rename-conflicts`, conflicting files that block renames will be permanently removed from your local directory. These files cannot be recovered unless they exist in the remote Keboola project. Only use this option when you have no uncommitted local changes and are purely syncing UI modifications.
</div>

[Global Options](../../commands.md#global-options)

## Examples

```
➜ kbc pull --dry-run
Pulling objects to the local directory.
Plan for "pull" operation:
  × C main/extractor/keboola.ex-db-mysql/7511990/invoices
  × R main/extractor/keboola.ex-db-mysql/7511990/invoices/rows/customer
Pull done.
```

## Next Steps

- [All Commands](../../commands.md)
- [Init](init.md)
- [Push](push.md)
- [Diff](diff.md)
