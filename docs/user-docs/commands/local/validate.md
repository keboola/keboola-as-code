# Validate Local Project Command

**Validate the [local project directory](../../structure.md).**

```
kbc local validate [flags]
```

Or shorter:
```
kbc v [flags]
```

Validate the directory structure and file contents of the local directory. Configurations of components having a JSON schema
will be validated against the schema.

## Options

[Global Options](../../commands.md#global-options)

## Example

```
➜ kbc validate
Everything is good.
```

## Sub Commands

|---
| Command | Description
|-|-|-
| [kbc local validate config](validate/config.md) | Validate a configuration JSON file. |
| [kbc local validate row](validate/row.md) | Validate a configuration row JSON file. |
| [kbc local validate schema](validate/schema.md) | Validate a configuration/row JSON file by a JSON schema file. |


## Next Steps

- [All Commands](../../commands.md)
- [Diff](../sync/diff.md)
- [Push](../sync/push.md)
- [Fix Paths](fix-paths.md)
