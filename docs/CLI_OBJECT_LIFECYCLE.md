# CLI Object Lifecycle and State Management

**CRITICAL READING for anyone working on CLI internals, mappers, or object state management.**

This document explains how objects (branches, configs, rows, notifications, etc.) are created, loaded, saved, and tracked in the Keboola CLI.

## Table of Contents

1. [Core Concepts](#core-concepts)
2. [Object Lifecycle](#object-lifecycle)
3. [State vs Manifest](#state-vs-manifest)
4. [Registry.Set() - CRITICAL](#registryset---critical)
5. [Mappers and Their Timing](#mappers-and-their-timing)
6. [Pull Operation Flow](#pull-operation-flow)
7. [Push Operation Flow](#push-operation-flow)
8. [Common Pitfalls](#common-pitfalls)

---

## Core Concepts

### Object Components

Each CLI object has THREE components:

1. **Object** (`*model.Notification`, `*model.Config`, etc.)
   - The actual data model with fields (event, recipient, filters, etc.)
   - Contains business logic and validation rules

2. **Manifest** (`*model.NotificationManifest`, `*model.ConfigManifest`, etc.)
   - Metadata about WHERE the object is stored (paths, IDs)
   - Tracked in `.keboola/manifest.json` for persistence across runs
   - Parent-child relationships (Config → Notifications, Config → Rows)

3. **State** (`*model.NotificationState`, `*model.ConfigState`, etc.)
   - Combines Manifest + Object (Local and/or Remote versions)
   - Tracks whether object exists locally, remotely, or both
   - Managed by the Registry

### State Fields

```go
type NotificationState struct {
    *NotificationManifest          // WHERE it is
    Local  *Notification            // Local filesystem version
    Remote *Notification            // Remote API version
}
```

**IMPORTANT:** During operations:
- **Pull**: Remote is set first, Local is set during save
- **Push**: Local is set first, Remote is set after API call
- **Both exist**: Both are set, diff engine compares them

---

## Object Lifecycle

### Creation Flow (Pull Operation)

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Remote Load (remote/notification.go)                      │
│    - Fetch from API                                           │
│    - u.loadObject(notification) creates Manifest + State     │
│    - loadObject sets Remote = Local = notification (copy)    │
│    - Set parent path on manifest                             │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. MapBeforeLocalSave (mapper/notification/local_save.go)    │
│    - Called ONCE PER NOTIFICATION (not per config)           │
│    - Writes config.json: {event, filters, recipient}         │
│    - Writes meta.json: {}                                     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. File Writing + Manifest Save (manifest/file.go)           │
│    - Write notification files to notifications/sub-{id}/     │
│    - setRecords() populates ConfigManifest.Notifications     │
│    - Result: {"notifications": [{"id": "...", "path": ...}]} │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. Validation (state/state.go:validateLocal)                 │
│    - Check Local state for all objects                       │
│    - Validate required fields (event, recipient)             │
└─────────────────────────────────────────────────────────────┘
```

### Loading Flow (After Files Exist)

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Manifest Load (project/manifest/file.go)                  │
│    - Read .keboola/manifest.json                             │
│    - Create Manifests for all tracked objects                │
│    - Registry.GetOrCreateFrom() for each                     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. Local Load (state/local/manager.go)                       │
│    - Scan filesystem for object directories                  │
│    - Match paths to manifests                                │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. MapAfterLocalLoad (mapper/notification/local_load.go)     │
│    - Called ONCE PER NOTIFICATION                            │
│    - Reads config.json → notification.Event, Recipient       │
│    - No auto-filters in local file (added during push only)  │
└─────────────────────────────────────────────────────────────┘
```

---

## State vs Manifest

### State (Registry)

**What it is:** In-memory collection of ALL objects currently loaded

**Where:** `internal/pkg/state/registry/registry.go`

**Purpose:**
- Track all objects in this session
- Provide lookup by key
- Manage object relationships

**Access:**
- `state.All()` - Get all objects
- `state.Get(key)` - Get specific object
- `state.Set(objectState)` - Add NEW object (fails if exists!)

### Manifest

**What it is:** Persistent record of which objects exist in the project

**Where:** `.keboola/manifest.json` (file on disk)

**Purpose:**
- Remember object IDs across runs
- Track parent-child relationships (config → rows, config → notifications)
- Enable diff operations (detect deletions)

**Structure:**
```json
{
  "configurations": [
    {
      "id": "config-123",
      "path": "extractor/ex-generic-v2/my-config",
      "rows": [{"id": "row-456"}],
      "notifications": [{"id": "sub-789", "path": "notifications/sub-789"}]
    }
  ]
}
```

**When populated:**
- `ConfigManifest.Rows` - During manifest save via `setRecords()`
- `ConfigManifest.Notifications` - During manifest save via `setRecords()` (NOT in mapper)
- Saved by `manifest.Save()` which calls `setRecords(m.All())`

---

## Registry.Set() - CRITICAL

### **RULE: Set() is ONLY for NEW objects**

```go
// ✅ CORRECT - Adding new object via loadObject (preferred pattern)
// loadObject handles manifest creation, Registry.GetOrCreateFrom(), setting Remote+Local
if err := u.loadObject(notification); err != nil {
    return err
}

// ❌ WRONG - Trying to update existing object
existingState, _ := registry.Get(key)
existingState.Local = newValue
registry.Set(existingState)  // FAILS: "already exists"

// ✅ CORRECT - Updating existing object
existingState, _ := registry.Get(key)
existingState.Local = newValue  // Just assign, no Set() call needed
```

### Why Set() Fails on Existing Objects

From `registry.go:304-306`:
```go
if _, found := s.objects.Get(key.String()); found {
    return errors.Errorf(`object "%s" already exists`, key.Desc())
}
```

**To modify existing objects: Just update fields directly**

The Registry holds references, so changes to the object are immediately visible.

---

## Mappers and Their Timing

### Mapper Types

#### LocalSaveMapper (MapBeforeLocalSave)
**When:** Before object is written to disk
**Use for:**
- Transform object → file content
- Create additional files (meta.json, description.md)
- Called ONCE PER OBJECT, not once per parent

#### LocalLoadMapper (MapAfterLocalLoad)
**When:** After files are read from disk
**Use for:**
- Parse file content → object
- Normalize/validate data

#### RemoteLoadMapper (MapAfterRemoteLoad)
**When:** After object is fetched from API
**Use for:** Rarely used, most remote work happens in remote/manager.go

#### BeforePersistMapper (MapBeforePersist)
**When:** Before new object is added to manifest
**Use for:** Modify manifest record before first save

### Execution Order (Pull)

```
Remote Load (u.loadObject per notification)
    ↓
MapBeforeLocalSave (called per notification)
    ↓
Write Files (config.json + meta.json per notification)
    ↓
Write Manifest (manifest.json) ← setRecords() populates ConfigManifest.Notifications HERE
    ↓
Validation
```

### Execution Order (Load from disk)

```
Manifest Load (reads notification IDs and paths)
    ↓
Local Load (scan filesystem, create states from manifest)
    ↓
MapAfterLocalLoad (reads config.json per notification)
    ↓
Validation
```

---

## Pull Operation Flow

### Step-by-Step: Pulling Notifications

```go
// 1. REMOTE LOAD (remote/notification.go:loadNotifications)
notification := model.NewNotification(apiSubscription)
notification.BranchID = parentConfig.BranchID  // From parent config manifest
notification.ComponentID = parentConfig.ComponentID
notification.ConfigID = parentConfig.ID

// loadObject creates manifest + state (Remote=Local=notification)
if err := u.loadObject(notification); err != nil {
    return err
}

// Set parent path after loadObject (needed for correct directory placement)
notificationState, found := u.state.Get(notification.Key())
if !found {
    continue
}
notificationState.Manifest().SetParentPath(parentConfig.Path())


// 2. MAP BEFORE LOCAL SAVE (mapper/notification/local_save.go)
// Called ONCE for this specific notification (NOT iterating all states)
func (m *mapper) MapBeforeLocalSave(_ context.Context, recipe *model.LocalSaveRecipe) error {
    notification, ok := recipe.Object.(*model.Notification)
    if !ok {
        return nil  // Skip non-notifications
    }
    // Write config.json: {event, filters, recipient, expiresAt}
    m.saveConfigFile(recipe, notification)
    // Write meta.json: {}
    m.saveMetaFile(recipe)
    return nil
}


// 3. MANIFEST SAVE (project/manifest/file.go:setRecords)
// setRecords() iterates all states and populates ConfigManifest.Notifications
// Result: manifest.json has {"notifications": [{"id": "sub-123", "path": "notifications/sub-123"}]}
```

### Why This Flow?

1. **Remote load creates states** - `loadObject` handles all manifest/state creation
2. **Mapper writes per-notification files** - Each notification gets its own `config.json` + `meta.json`
3. **Manifest save populates tracking** - `setRecords()` collects all notifications per config

**CRITICAL:** `ConfigManifest.Notifications` is populated by `setRecords()` during manifest save, NOT by the mapper or remote load.

---

## Push Operation Flow

### Step-by-Step: Pushing Notifications

```go
// 1. LOCAL LOAD (already happened)
// Manifest has {id, path} for each notification
// local_load.go reads config.json → notification.Event, Recipient (no auto-filters in local)
// State has Local set, Remote = nil (for new) or Remote set (for existing, from prior pull)


// 2. DIFF ENGINE (internal/pkg/diff)
// Compares Local vs Remote for each object
// New (no Remote): CREATE
// Changed (has Remote, fields differ): shows "changed: filters, recipient" etc.
// Note: on repeated push without pull, diff shows "changed: filters" because
//       local has no auto-filters (branch.id, job.component.id, job.configuration.id)
//       while remote has them. This is expected behavior.


// 3. REMOTE SAVE (remote/manager.go via remote/notification.go)
// New notification: createNotificationRequest() auto-adds branch/component/config filters
req := u.createNotificationRequest(notification)
// Updated notification: delete-old (WithOnSuccess callback) then create-new
// This is because the notifications API has no update endpoint.


// 4. SUCCESS CALLBACK
// Assigns API-assigned ID and sets remote state
notification.ID = created.ID
notification.CreatedAt = created.CreatedAt
notificationState.SetRemoteState(remoteNotification)
u.changes.AddCreated(notificationState)

// ❌ DO NOT: Registry.Set(notificationState)
// State already exists from local load!


// 5. NO LOCAL FILE UPDATE
// Push never writes local files
// IDs in manifest.json are updated for new notifications via manifest.Save()
```

### Auto-Filter Behavior

Users write only `event` + `recipient` (and optionally custom `filters`) in `config.json`. During push, three standard filters are **auto-populated** by `createNotificationRequest()`:

- `branch.id` — from the notification's `BranchID`
- `job.component.id` — from the notification's `ComponentID`
- `job.configuration.id` — from the notification's `ConfigID`

These filters are required by the API to scope the notification to a specific config. They are appended before any user-defined filters.

The API returns all filters (auto + user) in the response, so after a `pull`, `config.json` contains all three auto-filters plus any user-defined ones.

**Consequence:** On a repeated `push` without `pull`:
- Local `config.json` has no auto-filters (they were never written locally)
- Remote notification has all three auto-filters
- Diff engine shows `changed: filters`
- This is **expected behavior**, not a bug

### Delete-Then-Create for Updates

The notifications API has no update endpoint. Changes are implemented as:
1. Delete the old notification subscription
2. Create a new one with the changed values

This is implemented via `WithOnSuccess` callback chaining in `UnitOfWork.SaveObject`:
```go
// Pseudocode in remote/manager.go
deleteReq.WithOnSuccess(func(...) {
    // On successful delete, create the new one
    return createReq.Send(ctx)
})
```

---

## Common Pitfalls

### ❌ Pitfall 1: Calling Set() on Existing Objects

```go
// WRONG
existingState, _ := m.state.Get(key)
existingState.Local = newValue
m.state.Set(existingState)  // ERROR: already exists

// CORRECT
existingState, _ := m.state.Get(key)
existingState.Local = newValue  // Just assign, that's it!
```

### ❌ Pitfall 2: Manual State Construction Instead of loadObject

```go
// WRONG - Manual manifest/state construction (old pattern)
manifest := &model.NotificationManifest{...}
manifest.SetParentPath(...)
state := &model.NotificationState{
    NotificationManifest: manifest,
    Remote: notification,
    Local:  notification,
}
u.state.Set(state)

// CORRECT - Use loadObject (handles everything)
if err := u.loadObject(notification); err != nil {
    return err
}
notificationState, _ := u.state.Get(notification.Key())
notificationState.Manifest().SetParentPath(parentConfig.Path())
```

### ❌ Pitfall 3: Populating ConfigManifest.Notifications in Mapper

```go
// WRONG - Mapper should not populate ConfigManifest.Notifications
func (m *mapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
    config, ok := recipe.Object.(*model.Config)
    if !ok { return nil }
    // This is WRONG for notifications - setRecords() handles it during manifest.Save()
    for _, notifState := range allNotifications {
        config.Manifest.Notifications = append(config.Manifest.Notifications, ...)
    }
}

// CORRECT - setRecords() in manifest/file.go populates this automatically
// Just write the per-notification files in MapBeforeLocalSave:
func (m *mapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
    notification, ok := recipe.Object.(*model.Notification)
    if !ok { return nil }
    m.saveConfigFile(recipe, notification)
    m.saveMetaFile(recipe)
    return nil
}
```

### ❌ Pitfall 4: Confusing State.All() with Manifest.All()

```go
// State.All() - Returns ObjectState (has Local/Remote)
for _, objectState := range state.All() {
    notif := objectState.(*NotificationState).Local
}

// Manifest.All() - Returns ObjectManifest (has paths/IDs only)
for _, manifest := range manifest.All() {
    notifManifest := manifest.(*NotificationManifest)
    // No access to actual notification data here!
}
```

---

## Testing Checklist

When implementing a new object type:

- [ ] Remote load uses `u.loadObject()` (not manual manifest/state construction)
- [ ] `loadObject()` called before `SetParentPath()` on manifest
- [ ] `MapBeforeLocalSave` writes per-object files (not per-parent files)
- [ ] `MapAfterLocalLoad` reads per-object files, no registry manipulation
- [ ] `ConfigManifest.Notifications`/`Rows` populated by `setRecords()`, not by mapper
- [ ] Manifest custom JSON marshaling handles your object type
- [ ] `manifest/file.go:records()` includes your object type
- [ ] `manifest/file.go:setRecords()` handles your object type

---

## References

- State Registry: `internal/pkg/state/registry/registry.go`
- Manifest File: `internal/pkg/project/manifest/file.go`
- Mapper Interfaces: `internal/pkg/mapper/mapper.go`
- Example Notification Mappers:
  - `internal/pkg/mapper/notification/local_save.go`
  - `internal/pkg/mapper/notification/local_load.go`
  - `internal/pkg/state/remote/notification.go`

---

## Quick Reference Card

```
┌─────────────────────────────────────────────────────────────┐
│ GOLDEN RULES                                                  │
├─────────────────────────────────────────────────────────────┤
│ 1. Registry.Set() ONLY for NEW objects                       │
│ 2. Update existing: just assign fields, no Set()             │
│ 3. Pull: use loadObject() — creates manifest+state for you   │
│ 4. ConfigManifest.Notifications: populated by setRecords()   │
│ 5. MapBeforeLocalSave: called per-object, write object files │
│ 6. MapAfterLocalLoad: called per-object, read object files   │
│ 7. Validation needs Local to be set (not nil)                │
└─────────────────────────────────────────────────────────────┘
```
