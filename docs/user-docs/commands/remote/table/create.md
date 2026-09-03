# Create Table

To create a [table](https://help.keboola.com/storage/tables/) in Keboola Storage directly from the command line interface, use the following command:

```
kbc remote create table [flags]
```

### Options

`--bucket <string>`
: Specifies the bucket ID where the table will be created.

`--columns <string>`
: Defines a comma-separated list of column names for the table.

`--columns-from <string>`
: Indicates the path to the column definition file in json.

`--name <string>`
: Sets the name of the new table.

`--primary-key <string>`
: Determines a comma-separated list of columns to be used as the primary key.

`--options-from <string>`
: The path to the table definition file with backend-specific options.  
: This flag is enabled only for projects with the BigQuery backend and must be combined with a `--columns-from` flag because these settings must have specific column types.

[Global Options](../../../commands.md#global-options)

### Usage Examples

**Creating a table without defining column types:**

```
➜ kbc remote create table

? Select a bucket:  [Use arrows to move, type to filter]
  bucket1 (in.c-bucket1)
> bucket2 (in.c-bucket2)

Enter the table name.
? Table name: my-table

Want to define column types?
? Columns Types Definition: [? for help] (Y/n)
```
If you want to skip defining column types, select `n/N` when prompted and enter the names of the columns.
```
Want to define column types?
? Columns Types Definition: No

Enter a comma-separated list of column names.
? Columns: id,name,age

? Select columns for the primary key:  [Use arrows to move, space to select]
> [x]  id
  [ ]  name
  [ ]  age

Created table "in.c-bucket2.my-table".
```
**Defining column types:**

To define column types, select `y/Y`. Then, start an editor. 

```
Want to define column types?
? Columns Types Definition: Yes

Columns definition from file
? Columns definition from file: [Enter to launch editor]
```
**Edit the YAML file in the editor:**

Edit or replace this part of the text with your definition. Keep the same format. Then save your changes and close the editor.

```
- name: id
  definition:
    type: VARCHAR
    nullable: false
    length: 500
  basetype: STRING
- name: name
  definition:
    type: VARCHAR
    nullable: true
  basetype: STRING
```
```
Columns definition from file
? Columns definition from file: <Received>

? Select columns for the primary key:  [Use arrows to move, space to select]
> [x]  id

Created table "in.c-bucket2.my-table".
```
**Defining column types using a JSON file:**

```
kbc remote create table --columns-from <definition.json> [flags]
```
Example JSON file:
```json
[
    {
      "name": "id",
      "definition": {
        "type": "VARCHAR",
        "nullable": false,
        "length": "500"
      },
      "basetype": "STRING"
    },
    {
      "name": "name",
      "definition": {
        "type": "VARCHAR",
        "nullable": true
      },
      "basetype": "STRING"
    }
]
```
**Writing a JSON file that defines Bigquery settings:**

```
kbc remote create table --columns-from <definition.json> --options-from <options.json> [flags]
```
Example JSON file:
```json
{
  "timePartitioning": {
    "type": "DAY",
    "expirationMs": 864000000,
    "field": "time"
  },
  "clustering": {
    "fields": [
      "id"
    ]
  },
  "rangePartitioning": {
    "field": "id",
    "range": {
      "start": 0,
      "end": 10,
      "interval": 1
    }
  }
}
```



## Next Steps

- [All Commands](../../../commands.md)
- [Create a Bucket](../create/bucket.md)
- [Table Upload](upload.md)
