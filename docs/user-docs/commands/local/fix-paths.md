# Fix Paths Command

**Ensure that all local paths match [configured naming](../../structure.md#naming).**

```
kbc local fix-paths [flags]
```

The command unifies names of configurations, rows, and other directories based on [configured naming](../../structure.md#naming).
For example, if the configuration name in `meta.json` changes, this command renames the directory by that name.
It is run automatically after [pull](../sync/pull.md). 

## Options

`--dry-run`
: Preview all paths that would be affected

[Global Options](../../commands.md#global-options)

## Examples

When you have a config and rename it in its `meta.json`, run the command afterwards. It will rename the directory:

```
➜ kbc fix-paths --dry-run
Plan for "rename" operation:
  - main/extractor/ex-generic-v2/{wiki-001 -> wiki-2}
Dry run, nothing changed.
Fix paths done.
```

## Next Steps

- [All Commands](../../commands.md)
- [Persist](persist.md)
