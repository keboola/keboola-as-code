# Validate Schema Command

**Validate a [configuration](https://help.keboola.com/extend/common-interface/config-file/)/[row](https://help.keboola.com/components/#configuration-rows) JSON file by a JSON schema file.**

```
kbc local validate schema schema.json config.json [flags]
```

Validate the content of the specified JSON file
against the specified JSON schema file.

The JSON schema should contain a schema for the `parameters` key,
just like the configuration/row schema in a [component](https://help.keboola.com/extend/component/) definition.

The main purpose of this command is to **test 
a new JSON schema before it is changed in a component definition**.
It can be used both in a project [local directory](../../../structure.md) and also separately.

## Options

[Global Options](../../../commands.md#global-options)

## Example

A successful run, the JSON file is valid:
```
➜ kbc local validate schema schema.json config.json
Validation done.
```

A validation error:
```
➜ kbc local validate schema schema.json config.json
Error: missing properties: "subscriptionId"
```

## Next Steps

- [All Commands](../../../commands.md)
- [Validate Local Project](../validate.md)
