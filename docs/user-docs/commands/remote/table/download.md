# Table Download

**Download data from a [table](https://help.keboola.com/storage/tables/).**

```
kbc remote table download [table] [flags]
```

`file`
: Path and/or name of the source file. If `-`, input is expected from standard input, so the command is pipeable.

`table`
: ID of the destination table.

### Options

`-H, --storage-api-host <string>`
: Keboola instance URL, e.g., `connection.keboola.com`

`-o, --output <string>`
: Path and/or name of the destination file (if the file is not sliced) or directory (if the file is sliced). If `-`, output goes to `stdout` without any extra text, so the command is pipeable.

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

`--header`
: First line of the CSV file contains column names.

`--limit <int>`
: Limit the number of exported rows. A value of 0 means no limit. (default 0)

`--order <string>`
: Order the data by one or more columns.

  Accepts a comma-separated list of column+order pairs, such as `First_Name,Last_Name=desc`.
  If the order for a column is not specified, it defaults to ascending, such as `First_Name` in the example above.

`--where <string>`
: Filter the data.

  Accepts a semicolon-separated list of expressions, each of which specifies a column and a comparison to one or more values, such as `First_Name=Ivan,Pavel;Birth_Date>=1990-01-01`

`--allow-sliced`
: Allow sliced files to appear sliced locally. (default false)

  By default, sliced files are stitched together to form a single file.
  If this flag is set when downloading a sliced file, the resulting file will instead be stored as a directory, and each slice will be stored as a separate file in that directory.


[Global Options](../../../commands.md#global-options)

### Examples

Download 2000 rows from a table:
```
➜ kbc remote table download in.c-gdrive.account -o account.csv --limit 2000
Unloading table, please wait.
Table "in.c-gdrive.account" unloaded to file "734370450".
File "734370450" downloaded to "account.csv".
```

## Next Steps

- [All Commands](../../../commands.md)
- [Learn more about Tables](https://help.keboola.com/storage/tables/)
