# File Upload

**Upload a [file](https://help.keboola.com/storage/files/) to [Storage](https://help.keboola.com/storage/).**

```
kbc remote file upload [flags]
```

### Options

`-H, --storage-api-host <string>`
: Keboola instance URL, e.g., `connection.keboola.com`

`--data <string>`
: Path and/or name of the source file. If `-`, input is expected from standard input, so the command is pipeable.

`--file-name <string>`
: Name of the file to be created

`--file-tags <string>`
: Comma-separated list of tags

[Global Options](../../../commands.md#global-options)

### Examples

```
➜ $ kbc remote file upload --name file1 --data ./name.csv --tags tag1,tag2
File "file1" uploaded with file id "1234567".
```

If you don't specify the options, the command will ask you for them. 
```
➜ $ kbc remote file upload
? Enter a name for the file: file1

Enter a path for the file input or - to read from standard input.
? File: name.csv

Enter a comma-separated list of tags.
? Tags: tag1,tag2

File "file1" uploaded with file id "1234567".
```

If you specify `-` as input, the file will be read from standard input. 
```
➜ cat ./name.csv | kbc remote file upload --file-name file1 --data -
File "file1" uploaded with file id "1234567". 
```

## Next Steps

- [All Commands](../../../commands.md)
- [Learn more about Files Storage](https://help.keboola.com/storage/files/)
