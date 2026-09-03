# Table Preview

**Preview up to 1000 rows from a [table](https://help.keboola.com/storage/tables/).**

```
kbc remote table preview [table] [flags]
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
: Output format. Supported formats are `json`, `csv`, and `pretty`. (default `pretty`)

  `csv` is formatted according to [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt).

  `json` is formatted as follows:
  ```json
  {
    "columns": ["Id", "Name", "Region"],
    "rows": [
      ["Id0", "Name0", "Region0"],
      ["Id1", "Name1", "Region1"],
      ["Id2", "Name2", "Region2"]
    ]
  }
  ```

`-o, --out <string>`
: Write the data to a file. Fails if the file already exists.

`--force`
: When combined with `--out`, the file will be overwritten if it already exists.

`--limit <int>`
: Limit the number of exported rows (maximum 1000, default 100).

`--order <string>`
: Order the data by one or more columns.
  
  Accepts a comma-separated list of column+order pairs, such as `First_Name,Last_Name=desc`.
  If the order for a column is not specified, it defaults to ascending, such as `First_Name` in the example above.

`--where <string>`
: Filter the data.

  Accepts a semicolon-separated list of expressions, each of which specifies a column and a comparison to one or more values, such as `First_Name=Ivan,Pavel;Birth_Date>=1990-01-01`

[Global Options](../../../commands.md#global-options)

### Examples

Preview a table in the terminal:
```
➜ kbc remote table preview in.c-demo-keboola-ex-google-drive-1234567.account
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Id                               ┃ Name              ┃ Region  ┃ First_Order  ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╋━━━━━━━━━━━━━━━━━━━╋━━━━━━━━━╋━━━━━━━━━━━━━━┫
┃ f030ed64cbc8babbe50901a26675a2ee ┃ CSK Auto          ┃ US West ┃ 2015-01-23   ┃
┃ 06c0b954b0d2088e3da2132d1ba96f31 ┃ AM/PM Camp        ┃ Global  ┃ 2015-02-04   ┃
┃ fffe0e30b4a34f01063330a4b908fde5 ┃ Super Saver Foods ┃ Global  ┃ 2015-02-06   ┃
┃ 33025ad4a425b6ee832e76beb250ae1c ┃ Netcore           ┃ Global  ┃ 2015-03-02   ┃
┃ ...                              ┃ ...               ┃ ...     ┃ ...          ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━┻━━━━━━━━━━━━━━┛
```

Preview a table with where filters and ordering, and output the result to a CSV
file:
```
➜ kbc remote table preview in.c-demo-keboola-ex-google-drive-1234567.account \
  --where Age>23 \
  --order Age=asc \
  --limit 1000 \
  --format csv \
  --out accounts-preview.csv
Fetching the data, please wait.
Table "in.c-gdrive.account" preview successfully written to "accounts-preview.csv".
```

Preview a table in the terminal without knowing its ID:
```
➜ kbc remote table preview
? Table:  [Use arrows to move, type to filter]
> in.c-my-bucket.data
  in.c-demo-keboola-ex-google-drive-1234567.account
  in.c-facebook-extractor.uses
  ...

(down arrow pressed)

➜ kbc remote table preview
? Table:  [Use arrows to move, type to filter]
  in.c-my-bucket.data
> in.c-demo-keboola-ex-google-drive-1234567.account
  in.c-facebook-extractor.uses
  ...

(enter pressed)

➜ kbc remote table preview
? Table: in.c-demo-keboola-ex-google-drive-1234567.account
Fetching the data, please wait.
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━┓
┃ Id                               ┃ Name              ┃ Region  ┃ First_Order  ┃
┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╋━━━━━━━━━━━━━━━━━━━╋━━━━━━━━━╋━━━━━━━━━━━━━━┫
┃ f030ed64cbc8babbe50901a26675a2ee ┃ CSK Auto          ┃ US West ┃ 2015-01-23   ┃
┃ 06c0b954b0d2088e3da2132d1ba96f31 ┃ AM/PM Camp        ┃ Global  ┃ 2015-02-04   ┃
┃ fffe0e30b4a34f01063330a4b908fde5 ┃ Super Saver Foods ┃ Global  ┃ 2015-02-06   ┃
┃ 33025ad4a425b6ee832e76beb250ae1c ┃ Netcore           ┃ Global  ┃ 2015-03-02   ┃
┃ ...                              ┃ ...               ┃ ...     ┃ ...          ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━┻━━━━━━━━━━━━━━┛
```

## Next Steps

- [All Commands](../../../commands.md)
- [Learn more about Tables](https://help.keboola.com/storage/tables/)
