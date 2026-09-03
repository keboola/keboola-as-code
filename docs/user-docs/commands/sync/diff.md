# Diff Command

**Show differences between a [local directory](../../structure.md) and a [project](../../README.md#subsystems).**

```
kbc sync diff [flags]
```

Or shorter:
```
kbc diff [flags]
kbc d [flags]
```

## Options

`--details`
: Show changed fields

[Global Options](../../commands.md#global-options)

## Examples

When you change a configuration option of one component (e.g., an output table for a sheet 
in the [Google Drive extractor](https://help.keboola.com/components/extractors/storage/google-drive/)), the output will look like this:

```
➜ kbc diff
* changed
- remote state
+ local state

Diff:
* R main/extractor/keboola.ex-aws-s3/my-aws-s-3-data-source/rows/share-cities-2 | changed: configuration
+ C main/extractor/keboola.ex-db-mysql/invoices
+ R main/extractor/keboola.ex-db-mysql/invoices/rows/customer

Use --details flag to list the changed fields.
```

If you want more details:

```
➜ kbc diff --details
* changed
- remote state
+ local state

Diff:
* R main/extractor/keboola.ex-aws-s3/my-aws-s-3-data-source/rows/jakubm-share-cities-2
  configuration:
    parameters.key:
      - cities2.csv
      + cities.csv
+ C main/extractor/keboola.ex-db-mysql/invoices
+ R main/extractor/keboola.ex-db-mysql/invoices/rows/customer
```

## Next Steps

- [All Commands](../../commands.md)
- [Init](init.md)
- [Pull](pull.md)
- [Push](push.md)
