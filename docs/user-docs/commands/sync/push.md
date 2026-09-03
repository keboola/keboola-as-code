# Push Command

**Sync a [local directory](../../structure.md) to the [project](../../README.md#subsystems).**

```
kbc sync push [flags]
```

Or shorter:
```
kbc push [flags]
kbc ph [flags]
```

The project state will be overwritten to match the local state.

## Options

`--dry-run`
: Preview all changes

`--encrypt`
: Encrypt unencrypted values before the push

`--force`
: Delete configurations missing in the local directory

[Global Options](../../commands.md#global-options)

## Example

When you [create a configuration](../local/create/config.md) of the MySQL extractor, the command will look like this:

```
➜ kbc push --dry-run

Plan for "push" operation:
  + C main/extractor/keboola.ex-db-mysql/7511990/invoices
  + R main/extractor/keboola.ex-db-mysql/7511990/invoices/rows/customer
Dry run, nothing changed.
Push done.
```

## Next Steps

- [All Commands](../../commands.md)
- [Init](init.md)
- [Pull](pull.md)
- [Diff](diff.md)
