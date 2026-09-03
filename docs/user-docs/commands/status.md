# Status Command

**Show information about the current working directory.**

```
kbc status [flags]
```

## Options

[Global Options](../commands.md#global-options)

## Examples

Status of a project directory:
```
➜ kbc status
Project directory:  /home/kbc-project
Working directory:  .
Manifest path:      .keboola/manifest.json
```

Status of a template repository directory:
```
➜ kbc status
Repository directory:  /home/kbc-repository
Working directory:     .
Manifest path:         .keboola/repository.json
```

Status of a template directory:
```
➜ kbc status
Template directory:    /home/kbc-repository/my-template/v1
Working directory:     .
Manifest path:         src/manifest.jsonnet
```

Status of an unknown directory:
```
Directory "/home/kbc-test" is not a project or template repository.
```


## Next Steps

- [All Commands](../commands.md)
- [Init](sync/init.md)
