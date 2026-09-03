# Encrypt Command

Encrypt all [unencrypted secrets](https://help.keboola.com/extend/common-interface/config-file/#encryption) in the [local directory](../../structure.md).

```
kbc local encrypt [flags]
```

Or shorter:
```
kbc e [flags]
```

[Unencrypted secrets](https://help.keboola.com/extend/common-interface/config-file/#encryption) are values of properties prefixed by `#` that have not been encrypted 
yet. 

For example, `{"#someSecretProperty": "secret value"}`  
will be transformed into `{"#someSecretProperty": "KBC::ProjectSecure::<encryptedcontent>"}`.

## Options

`--dry-run`
: Preview all values that would be affected

[Global Options](../../commands.md#global-options)

## Examples

Let's say you create a configuration for the MySQL extractor:

```json
{
  "parameters": {
    "host": "our.mysql.server.dev",
    "user": "keboola",
    "#password": "super-secret"
  }
}
```

The preview will look like this:

```
➜ kbc encrypt --dry-run
Plan for "encrypt" operation:
  C main/extractor/keboola.ex-db-mysql/invoices
    parameters.#password
Dry run, nothing changed.
```

The actual encrypt command: 

```
➜ kbc encrypt
Plan for "encrypt" operation:
  C main/extractor/keboola.ex-db-mysql/invoices
    parameters.#password
Encrypt done.
```

And the configuration now looks like this:

```json
{
  "parameters": {
    "host": "our.mysql.server.dev",
    "user": "keboola",
    "#password": "KBC::ProjectSecureKV::eJxLtDK2qs60MrIutrI0s1K695WJQWmhYOI9j2l/twSJl0/nsf6auv/Fs7n5VWvj+tbwvtyz/PSh30Jz8/Y2B0QyPDwteXK/3d7GN55b/y3rK+BbXLF1ne5sg6/Lja/vfzlT4TbvXfkFIuHL9DU0knh8yvedF0lXss60MgbaZQS0Kz01Tzc1L7mosqAkv8jM0NIgzdTU0NQw1cACpMoEqMrYyEop1cjM0DjZzNzE0tLYxNTQMtEw0dLYKCnN0sDS1DjV3EzJuhYAzUBL0A=="
  }
}
```

## Next Steps

- [All Commands](../../commands.md)
- [Diff](../sync/diff.md)
- [Push](../sync/push.md)
