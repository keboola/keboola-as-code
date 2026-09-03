# Create Configuration Row

**Create an empty [configuration row](https://help.keboola.com/components/#configuration-rows).**

```
kbc local create row [flags]
```

Or shorter:
```
kbc create row [flags]
kbc c row [flags]
```

Create a new configuration row in your [local directory](../../../structure.md) and assign it a unique ID (i.e., the [persist](../persist.md)
command is called automatically). To save it to the project, run the [kbc sync push](../../sync/push.md) command afterwards. You will
be prompted for a name, a branch, and a component ID.

Some components have a default content that will be used (if specified by the component author).
For others, `config.json` will only contain an empty JSON document `{}`.

*Tip: You can create a new configuration row by copying an existing one and running the [persist](../persist.md) command.*

### Options

`-b, --branch string <string>`
: Id or name of the branch

`-c, --config <string>`
: Id or name of the configuration

`-n, --name <string>`
: Name of the new configuration row

[Global Options](../../../commands.md#global-options)

### Examples

```
➜ kbc create row

? Enter a name for the new config row customer

? Select the target branch Main (4908)

? Select the target config invoices (7475544)
Created new config row "main/extractor/keboola.ex-db-mysql/invoices/rows/customer"
```

```
➜ kbc create row -n customer -b main -c invoices
Created new config row "main/extractor/keboola.ex-db-mysql/invoices/rows/customer"
```

## Next Steps

- [All Commands](../../../commands.md)
- [Create Configuration](config.md)
- [Create Branch](../../remote/create/branch.md)
