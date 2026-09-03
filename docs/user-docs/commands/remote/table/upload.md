# Table Upload

**Upload a CSV file to a [table](https://help.keboola.com/storage/tables/).**

```
kbc remote table upload [table] [file] [flags]
```

`table`
: ID of the destination table.

`file`
: Path and/or name of the source file. If `-`, input is expected from standard input, so the command is pipeable.

### Options

`-H, --storage-api-host <string>`
: Keboola instance URL, e.g., `connection.keboola.com`

`--columns <string>`
: Comma-separated list of column names. If present, the first row in the CSV file is not treated as a header.

`--incremental-load`
: Data are either added to existing data in the table or replace the existing data.

`--primary-key <string>`
: Comma-separated list of columns representing the primary key for the newly created table if the table doesn't exist.

`--file-name <string>`
: Name of the file to be created

`--file-tags <string>`
: Comma-separated list of tags for the uploaded file

`--file-delimiter <string>`
: Delimiter of the CSV file. Default is `,`.

`--file-enclosure <string>`
: Enclosure of the CSV file. Default is `"`.

`--file-escaped-by <string>`
: Escape character of the CSV file. By default, no escaping is used. (***Note:** you can specify either the `enclosure` or `escapedBy` parameter, not both.*)

`--file-without-headers`
: States if the CSV file contains headers on the first row or not.



[Global Options](../../../commands.md#global-options)

### Examples

Create a table from a CSV file:
```
➜ kbc remote table upload in.c-users.accounts accounts.csv \
  --file-name accounts.csv
  --file-tags local-file
  --primary-key Id
File "accounts.csv" uploaded with file id "734370450".
Table "in.c-users.accounts" does not exist, creating it.
Bucket "in.c-users" does not exist, creating it.
Created new table "in.c-users.accounts" from file with id "734370450".
```

## Next Steps

- [All Commands](../../../commands.md)
- [Learn more about Tables](https://help.keboola.com/storage/tables/)
