# Table Unload

**Unload a [table](https://help.keboola.com/storage/tables/) into a [file](https://help.keboola.com/storage/files/).**

```
kbc remote table unload [table] [flags]
```

### Options

`-H, --storage-api-host <string>`
: Keboola instance URL, e.g., `connection.keboola.com`

`--changed-since <string>`
: Only export rows imported after this date.

  Date may be written in any format compatible with [strtotime](https://www.php.net/manual/en/function.strtotime.php).

`--changed-until <string>`
: Only export rows imported before this date.

  Date may be written in any format compatible with [strtotime](https://www.php.net/manual/en/function.strtotime.php).

`--columns <string>`
: Comma-separated list of columns to export.

`--format <string>`
: Output format. Supported formats are `json` and `csv`.
  
  The `json` format is only supported in projects with the Snowflake backend.

`--limit <int>`
: Limit the number of exported rows. A value of 0 means no limit. (default 0)

`--order <string>`
: Order the data by one or more columns.
  
  Accepts a comma-separated list of column+order pairs, such as `First_Name,Last_Name=desc`.
  If the order for a column is not specified, it defaults to ascending, such as `First_Name` in the example above.

`--where <string>`
: Filter the data.

  Accepts a semicolon-separated list of expressions, each of which specifies a column and a comparison to one or more values, such as `First_Name=Ivan,Pavel;Birth_Date>=1990-01-01`

`--timeout <string>`
: How long to wait for the storage job to finish (default `5m`)
  
  Specified as a sequence of decimal numbers with unit suffixes, e.g., `5m10s` or `1.5h`.  
  Available units are `ms`, `s`, `m`, and `h`.

`--async`
: Do not wait for the storage job to finish (default false)

[Global Options](../../../commands.md#global-options)

### Examples

Unload a table:
```
➜ kbc remote table unload in.c-gdrive.account
Unloading table, please wait.
Table "in.c-gdrive.account" unloaded to file "734370450".
```

Unload a table with where filters and ordering:
```
➜ kbc remote table unload in.c-gdrive.account \
  --where Age>23 \
  --order Age=asc \
  --limit 1000 \
  --format csv \
Unloading table, please wait.
Table "in.c-gdrive.account" unloaded to file "734370450".
```

Unload a table in the terminal without knowing its ID:
```
➜ kbc remote table unload
? Table:  [Use arrows to move, type to filter]
> in.c-my-bucket.data
  in.c-gdrive.account
  in.c-facebook-extractor.uses
  ...

(down arrow pressed)

➜ kbc remote table unload
? Table:  [Use arrows to move, type to filter]
  in.c-my-bucket.data
> in.c-gdrive.account
  in.c-facebook-extractor.uses
  ...

(enter pressed)

➜ kbc remote table unload
? Table: in.c-gdrive.account
Unloading table, please wait.
Table "in.c-gdrive.account" unloaded to file "734370450".
```

## Next Steps

- [All Commands](../../../commands.md)
- [Learn more about Tables](https://help.keboola.com/storage/tables/)
