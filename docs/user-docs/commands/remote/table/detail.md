# Table Detail

**Print [table](https://help.keboola.com/storage/tables/) details.**

```
kbc remote table detail [table] [flags]
```

### Options

`-H, --storage-api-host <string>`
: Keboola instance URL, e.g., `connection.keboola.com`

[Global Options](../../../commands.md#global-options)

### Examples

Print the details of a table:
```
➜ kbc remote table detail in.c-demo-keboola-ex-google-drive-1234567.account

Table "in.c-demo-keboola-ex-google-drive-1234567.account":
  Name: issues
  Primary key: Id, Name
  Columns: Id, Name, Region, First_Order
  Rows: 7801
  Size: 92 MB
  Created at: 2023-02-01T11:22:05.000Z
  Last import at: 2023-02-01T13:09:19.000Z
  Last changed at: 2023-02-01T13:09:19.000Z
```

Print the details of a table without knowing its ID:
```
➜ kbc remote table detail
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

➜ kbc remote table detail
? Table: in.c-demo-keboola-ex-google-drive-1234567.account

Table "in.c-demo-keboola-ex-google-drive-1234567.account":
  Name: issues
  Primary key: Id, Name
  Columns: Id, Name, Region, First_Order
  Rows: 7801
  Size: 92 MB
  Created at: 2023-02-01T11:22:05.000Z
  Last import at: 2023-02-01T13:09:19.000Z
  Last changed at: 2023-02-01T13:09:19.000Z
```

## Next Steps

- [All Commands](../../../commands.md)
- [Learn more about Tables](https://help.keboola.com/storage/tables/)
