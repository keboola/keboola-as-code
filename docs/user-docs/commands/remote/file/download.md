# File Download

**Download a [file](https://help.keboola.com/storage/files/) from [Storage](https://help.keboola.com/storage/).**

```
kbc remote file download [id] [flags]
```

### Options

`-H, --storage-api-host <string>`
: Keboola instance URL, e.g., `connection.keboola.com`

`-o, --output <string>`
: Path and/or name of the destination file (if the file is not sliced) or directory (if the file is sliced). If `-`, output goes to `stdout` without any extra text, so the command is pipeable.

`--allow-sliced`
: Allow sliced files to appear sliced locally. (default false)

  By default, sliced files are stitched together to form a single file.
  If this flag is set when downloading a sliced file, the resulting file will instead be stored as a directory, and each slice will be stored as a separate file in that directory.

`--without-decompress`
: Do not decompress the downloaded files or sliced files.

[Global Options](../../../commands.md#global-options)

### Examples

```
➜ $ kbc remote file download 1234567 -o name
File "1234567" downloaded to "name.csv".
```

If you don't specify the file ID, the command will let you select a file by name. 
```
➜ $ kbc remote file download
? File: <selection prompt>
? Enter a name for the destination: 

File "1234567" downloaded to "name.csv"
```

If you specify `-` as output, the file will be printed to standard output. 
```
➜ kbc remote file download 1234567 -o -
col1,col2,col3
val1,val2,val3
...
```

## Next Steps

- [All Commands](../../../commands.md)
- [Learn more about Files Storage](https://help.keboola.com/storage/files/)
