# Create Bucket

**Create a new [bucket](https://help.keboola.com/storage/buckets/) in Keboola Storage.**

```
kbc remote create bucket [flags]
```

### Options

`--stage <string>`
: Stage of the bucket, allowed values: `in`, `out`

`--display-name <string>`
: Display name of the bucket for the UI

`--name <string>`
: Name of the bucket

`--description <string>`
: Description of the bucket

[Global Options](../../../commands.md#global-options)

### Examples

```
➜ kbc remote create bucket

? Select a stage for the bucket:  [Use arrows to move, type to filter]
  in
> out
? Select a stage for the bucket: out

Enter a display name for the bucket: Bucket1

Enter a name for the bucket: bucket1

? Enter a description for the bucket: Test description

Created bucket "out.c-bucket1".
```

## Next Steps

- [All Commands](../../../commands.md)
- [Create Configuration](../../local/create/config.md)
- [Create Configuration Row](../../local/create/row.md)
