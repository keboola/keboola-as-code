# Variables Relations Validation

## Background: the keboola.variables parent-child model

In the Keboola platform, variable configs (`keboola.variables` component) belong to a parent config (e.g. a transformation). In the local CLI directory layout, the variables config lives *inside* the parent's folder:

```
my-transformation/
  variables/
    config.json         ← keboola.variables config
    variables/
      default/          ← variables values row
```

This parent-child ownership is expressed through relations stored on the objects:

| Relation type | Stored on | Direction | Storage |
|---|---|---|---|
| `variablesFor` | variables config | → parent config | manifest (local only) |
| `variablesFrom` | parent config | → variables config | API |
| `variablesValuesFor` | values row | → parent config | manifest |
| `variablesValuesFrom` | parent config | → values row | API |

The `variablesFor` relation on the variables config determines which parent folder it is placed in during sync. Because each object can have only one file-system path, **each variables config can have exactly one `variablesFor` relation** (i.e. one parent). This is enforced by `OneToXRelations()` in `internal/pkg/model/relation.go`.

## Why multiple parents can appear in the API

Keboola's Storage API allows a variables config to be referenced by more than one consumer via the project's configuration metadata. If a user creates such a setup through the API or the UI directly, the `keboola.variables` config will have multiple `variablesFrom` back-references, which the mapper translates into multiple `variablesFor` relations during the linking step.

## The `multiple parents` guard

`Relations.ParentKey()` (`internal/pkg/model/relation.go:66`) collects all relations that define a parent key. If more than one is found it returns a fatal error:

```
unexpected state: multiple parents defined by "relations" in <config desc>
```

This guard exists to protect the CLI sync engine: without it, path generation would be ambiguous and the local directory structure would be corrupted.

## The ordering bug that broke Templates

The relation mapper (`internal/pkg/mapper/relations/link.go`) processes objects in `AfterRemoteOperation` and `AfterLocalOperation`. It also validates them in the same loop iteration (link → validate per object). This is order-dependent:

1. If the variables config Y is iterated **before** its consumers X and Z, `validateRelations(Y)` runs when Y has zero `variablesFor` relations — nothing to clean up.
2. Later iterations for X and Z call `linkRelations`, which adds `VariablesForRelation` entries to Y.
3. After the mapper loop, `PathsGenerator.Invoke()` (called in `remote/manager.go`) calls `Y.ParentKey()`, finds two parents, and returns the fatal error.

CLI `pull` was unaffected in practice because the API happened to return consumer configs before variables configs, making consumers iterated first. The Templates service load path had a different iteration order and hit the bug.

## The fix: two-pass link and validate

`AfterRemoteOperation` and `AfterLocalOperation` were refactored into two explicit passes:

**Pass 1 — link all objects**
Run `linkRelations` for every loaded object. This creates all other-side relations (including adding `variablesFor` entries to every variables config) before any validation happens.

**Pass 2 — validate all objects**
Run `validateRelations` for every loaded object. With the complete relation graph now in place, duplicates are correctly detected, removed, and logged as warnings — regardless of the order objects appear in the API response.

This makes behaviour order-independent: both CLI and the Templates service emit a warning and continue when a variables config has multiple parents, instead of crashing. No new flags or special-casing were required.

## Why this is warning-only, not a hard error

The `validateRelations` function removes all duplicate relations and logs a warning. The invalid `variablesFor` entries are dropped, leaving the variables config parentless for path generation purposes (it is placed at the branch root rather than inside a parent folder). The project can still be synced — it just does not represent the shared-variables scenario in the local directory hierarchy.

Templates only reads remote state to discover existing configs and apply template changes on top. It never generates or syncs a local directory hierarchy, so the "ambiguous path" concern does not apply. Making this a hard error in the Templates code path only served to block users from using templates in projects with this configuration.

## Related files

| File | Role |
|---|---|
| `internal/pkg/mapper/relations/link.go` | Two-pass AfterRemoteOperation / AfterLocalOperation |
| `internal/pkg/model/relation.go` | `Relations.ParentKey()`, `OneToXRelations()` |
| `internal/pkg/model/variables.go` | `VariablesForRelation`, `VariablesFromRelation` definitions |
| `internal/pkg/state/remote/manager.go` | Sequencing of AfterRemoteOperation → PathsGenerator |
| `test/cli/pull/variables-used-twice/` | E2E test covering the shared-variables warning |
