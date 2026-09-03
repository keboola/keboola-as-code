# Persist Command

**Detect new directories with a [configuration](https://help.keboola.com/components/) or a [configuration row](https://help.keboola.com/components/#configuration-rows) in the [local directory](../../structure.md).**

```
kbc local persist [flags]
```

Or shorter:
```
kbc pt [flags]
```

Propagate changes in the [local directory](../../structure.md) to the manifest. When you manually create a configuration or a row (e.g., by 
copy & paste of another existing configuration), the command will add its record to the [manifest](../../structure.md#manifest) and generate a new ID. 
When you delete a configuration/row directory, the command will remove its record from the [manifest](../../structure.md#manifest). If you want 
to propagate the changes to the project, call the [push](../sync/push.md) command afterwards.

## Options

`--dry-run`
: Preview all changes

[Global Options](../../commands.md#global-options)

## Examples

When you copy & paste a directory of a MySQL extractor configuration, the command will look like this:

```
➜ kbc persist --dry-run
Plan for "persist" operation:
  + C main/extractor/keboola.ex-db-mysql/invoices 2
  + R main/extractor/keboola.ex-db-mysql/invoices 2/rows/customer
Dry run, nothing changed.
Persist done.
```

## Next Steps

- [All Commands](../../commands.md)
- [Diff](../sync/diff.md)
- [Push](../sync/push.md)
- [Fix Paths](fix-paths.md)
