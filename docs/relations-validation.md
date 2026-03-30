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

`Relations.ParentKey()` (`internal/pkg/model/relation.go:66`) collects all relations that define a parent key. If more than one is found it returns an error:

```
unexpected state: multiple parents defined by "relations" in <config desc>
```

This guard exists to detect invalid state in the relation graph.

## Config.ParentKey() and PathsGenerator

`Config.ParentKey()` (`internal/pkg/model/object.go`) calls `Relations.ParentKey()` to find the relation-defined parent. If that returns an error (multiple parents) or nil (no parent), it falls back to the structural parent — the branch.

`PathsGenerator.doUpdate()` (`internal/pkg/state/local/paths.go`) calls `object.ParentKey()` on every loaded object to build local directory paths. With the branch-fallback in `Config.ParentKey()`, a variables config that has multiple `variablesFor` relations is placed at the branch root (e.g. `main/variables/`) rather than inside any specific parent folder. This is non-fatal: PathsGenerator can complete and remote state loading succeeds.

## The ordering issue

The relation mapper (`internal/pkg/mapper/relations/link.go`) processes objects in `AfterRemoteOperation` and `AfterLocalOperation`. It links and validates each object in a single pass:

```
for each object:
    linkRelations(object)    // adds other-side relations to OTHER objects
    validateRelations(object) // validates THIS object's relations
```

This is order-dependent. If the variables config Y is iterated **before** its consumers X and Z:

1. `validateRelations(Y)` runs when Y has zero `variablesFor` relations — nothing to clean up.
2. Later iterations for X and Z call `linkRelations`, which adds `VariablesForRelation` entries to Y.
3. Y ends up with two `variablesFor` relations in the loaded remote state.
4. `PathsGenerator.doUpdate()` calls `Y.ParentKey()` — before the branch-fallback fix this was a fatal error.

CLI `pull` was unaffected in practice because the API happened to return consumer configs before variables configs, making consumers iterated first. The Templates service load path had a different iteration order and triggered step 3-4.

## The fix: branch fallback in Config.ParentKey()

`Config.ParentKey()` was changed to ignore the "multiple parents" error from `Relations.ParentKey()` and fall back to the branch (structural parent) instead of propagating the error. This makes PathsGenerator non-fatal for shared-variables configs regardless of iteration order.

The `validateRelations` function still detects the duplicate `variablesFor` relations and logs a warning. In CLI `pull`, this warning fires correctly when consumers are iterated before the variables config (the API's typical return order). In the Templates service load path the warning may fire differently (or not at all for the duplicate, depending on iteration order), but Templates is only reading remote state — it never generates a local directory hierarchy — so the warning is not critical there.

## Why this is warning-only, not a hard error

The `validateRelations` function removes all duplicate relations and logs a warning. The invalid `variablesFor` entries are dropped, leaving the variables config parentless for path generation purposes (it is placed at the branch root rather than inside a parent folder). The project can still be synced — it just does not represent the shared-variables scenario in the local directory hierarchy.

Templates only reads remote state to discover existing configs and apply template changes on top. It never generates or syncs a local directory hierarchy, so the "ambiguous path" concern does not apply. Making this a hard error in the Templates code path only served to block users from using templates in projects with this configuration.

## Related files

| File | Role |
|---|---|
| `internal/pkg/model/object.go` | `Config.ParentKey()` — branch fallback when multiple relation parents |
| `internal/pkg/model/relation.go` | `Relations.ParentKey()`, `OneToXRelations()` |
| `internal/pkg/mapper/relations/link.go` | Single-pass AfterRemoteOperation / AfterLocalOperation |
| `internal/pkg/model/variables.go` | `VariablesForRelation`, `VariablesFromRelation` definitions |
| `internal/pkg/state/remote/manager.go` | Sequencing of AfterRemoteOperation → PathsGenerator |
| `test/cli/pull/variables-used-twice/` | E2E test covering the shared-variables warning |
