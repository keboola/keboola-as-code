# CLI Service Context

This document provides architectural context for the Keboola CLI service (`kbc`) located in `/internal/pkg/service/cli/`.

## Overview

The CLI provides a command-line interface for managing Keboola projects as code. It enables:
- Bidirectional sync between local directory and Keboola Storage API
- Template management and instantiation
- Local validation and encryption
- Remote operations (create resources, run jobs, etc.)
- dbt integration
- CI/CD workflow generation

## Directory Structure

```
internal/pkg/service/cli/
├── cmd/                          # Command definitions (Cobra)
│   ├── cmd.go                    # Root command & common setup
│   ├── sync/                     # Sync commands (init, pull, push, diff)
│   │   ├── init/                 # Each command has its own package
│   │   │   ├── cmd.go           # Cobra command definition + flags
│   │   │   └── dialog.go        # Interactive prompts
│   │   ├── pull/
│   │   ├── push/
│   │   └── diff/
│   ├── local/                    # Local operations (create, validate, persist, encrypt, template)
│   │   ├── create/
│   │   ├── validate/
│   │   ├── persist/
│   │   ├── encrypt/
│   │   └── template/
│   ├── remote/                   # Remote API operations
│   │   ├── create/              # Create branches, buckets, tables, etc.
│   │   ├── file/                # File upload/download
│   │   ├── job/                 # Run jobs
│   │   ├── workspace/           # Workspace management
│   │   └── table/               # Table operations
│   ├── template/                # Template repository commands
│   │   ├── create/
│   │   ├── list/
│   │   ├── describe/
│   │   ├── test/
│   │   └── repository/
│   ├── dbt/                     # dbt integration
│   └── ci/                      # CI/CD workflow generation
│
├── dependencies/                # Dependency injection scopes
│   ├── provider.go              # Provider interface & implementation
│   ├── base.go                  # BaseScope (logger, fs, dialogs, etc.)
│   ├── cmdlocal.go              # LocalCommandScope (unauthenticated API access)
│   ├── cmdremote.go             # RemoteCommandScope (authenticated API access)
│   └── fsinfo.go                # Directory type detection helper
│
├── dialog/                      # Dialog helpers for interactive prompts
│   ├── dialog.go                # Main dialog wrapper
│   └── templatehelper/          # Template-specific dialog utilities
│
├── prompt/                      # Low-level prompt implementations
│   ├── prompt.go                # Prompt interface
│   ├── interactive/             # Real terminal prompts (survey library)
│   └── nop/                     # Non-interactive mode (no-op/defaults)
│
├── helpmsg/                     # Help message system
│   ├── helpmsg.go               # Message loader
│   └── msg/                     # Text files mirroring command structure
│       ├── app.txt              # Root help
│       ├── usage.txt            # Usage template
│       ├── sync/                # Help for sync commands
│       ├── local/
│       ├── remote/
│       └── template/
│
├── event/                       # Command event tracking (telemetry)
├── flag/                        # Global flag definitions
├── cmdconfig/                   # Config binding (flags + ENVs)
└── util/                        # CLI utilities
```

## Architecture Patterns

### 1. Command → Operation Separation

**Commands** (`cmd/**/cmd.go`):
- Define CLI interface (flags, args, help text)
- Handle flag binding via ConfigBinder
- Obtain appropriate dependency scope
- Call dialog functions for interactive input
- Delegate to operations for business logic
- Send telemetry events

**Operations** (`/pkg/lib/operation/**`):
- Pure business logic, reusable outside CLI
- Minimal dependency interfaces
- Testable with mocked dependencies
- No Cobra or CLI-specific code

**Example: `sync init` command flow**:
```go
// cmd/sync/init/cmd.go
func Command(p dependencies.Provider) *cobra.Command {
    return &cobra.Command{
        RunE: func(cmd *cobra.Command, args []string) error {
            // 1. Bind flags
            f := Flags{}
            p.BaseScope().ConfigBinder().Bind(ctx, cmd.Flags(), args, &f)

            // 2. Get dependencies (scope)
            d, _ := p.RemoteCommandScope(ctx, f.StorageAPIHost, f.StorageAPIToken)

            // 3. Ask for options (interactive)
            options, _ := AskInitOptions(ctx, d.Dialogs(), d, f)

            // 4. Delegate to operation
            return initOp.Run(ctx, options, d)
        },
    }
}
```

### 2. Three-Tier Dependency Injection

**BaseScope** (`dependencies/base.go`):
- Available to ALL commands
- Core dependencies: Logger, Filesystem, Dialogs, Clock, Process
- GlobalFlags, ConfigBinder, Environment
- FsInfo for directory detection
- No API access

**LocalCommandScope** (`dependencies/cmdlocal.go`):
- Extends BaseScope + PublicScope
- Storage API (read-only, no authentication)
- Components map (for schema validation)
- Template loading
- LocalProject/LocalTemplate/LocalRepository detection
- ProjectBackends, ProjectFeatures
- Used by: `local validate`, `local template`, etc.

**RemoteCommandScope** (`dependencies/cmdremote.go`):
- Extends LocalCommandScope + ProjectScope
- **Requires authentication** (Storage API token)
- Full API access with write permissions
- EventSender for telemetry
- Token validation
- Used by: `sync init/pull/push`, `remote create`, etc.

**Lazy Initialization**:
```go
type Lazy[T any] struct {
    init sync.Once
    value T
}

// Scopes are initialized only when needed
// Cached for command lifetime
```

**Provider Pattern**:
```go
type Provider interface {
    BaseScope() BaseScope
    LocalCommandScope(ctx, host, opts) (LocalCommandScope, error)
    RemoteCommandScope(ctx, host, token, opts) (RemoteCommandScope, error)
    LocalProject(ctx, ignoreErrors, host, token, ops) (*Project, RemoteCommandScope, error)
}
```

### 3. Flag Handling with ConfigMap

**ConfigMap Pattern** - Unified flag/env/default handling:
```go
type Flags struct {
    StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host"`
    StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token"`
    Branch          configmap.Value[string] `configKey:"branch" configShorthand:"b" configUsage:"branch name or ID"`
}

// Auto-generates Cobra flags from struct tags
configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

// Binds: CLI flags → ENV vars (KBC_ prefix) → Defaults
binder.Bind(ctx, cmd.Flags(), args, &f)
```

**Flag Precedence**: CLI flag > ENV var > Default value

**ENV Variable Mapping**: `--storage-api-host` → `KBC_STORAGE_API_HOST`

### 4. Interactive Dialogs

**Dialog Architecture**:

**Prompt Interface** (`prompt/prompt.go`):
```go
type Prompt interface {
    IsInteractive() bool
    Confirm(c *Confirm) bool
    Ask(q *Question) (result string, ok bool)
    Select(s *Select) (value string, ok bool)
    MultiSelect(s *MultiSelect) (result []string, ok bool)
    Editor(fileExt string, q *Question) (result string, ok bool)
}
```

**Two Implementations**:
- `interactive/` - Real terminal prompts using survey library
- `nop/` - Non-interactive mode (returns defaults or fails if required)

**Dialog Usage Pattern**:
```go
func AskInitOptions(ctx, dialogs, deps, flags) (Options, error) {
    // Use flag if set, otherwise prompt
    if !flags.Branch.IsSet() {
        branch, _ := dialogs.Select(&prompt.Select{
            Label:   "Select branch",
            Options: branchOptions,
        })
    }
    return Options{...}, nil
}
```

**Switching Modes**:
- Interactive by default
- Use `--non-interactive` flag to disable prompts
- CI environments auto-detect non-interactive mode

### 5. FsInfo - Directory Context Detection

**Purpose**: Validates the current directory type before command execution

**FsInfo Methods** (`dependencies/fsinfo.go`):
```go
type FsInfo interface {
    // Validators - return error if not correct type
    ProjectDir(ctx) (string, error)     // Requires .keboola/ directory
    TemplateDir(ctx) (string, error)    // Requires .keboola/template/ directory
    RepositoryDir(ctx) (string, error)  // Requires .keboola/repository/ directory
    EmptyDir(ctx) (string, error)       // Requires empty directory
    DbtProjectDir(ctx) (string, error)  // Requires dbt_project.yml

    // Detectors - return value without error
    IsProjectDir(ctx) bool
    IsTemplateDir(ctx) bool
    // ...
}
```

**Example Usage**:
```go
// cmd/sync/pull/cmd.go
RunE: func(cmd *cobra.Command, args []string) error {
    // Require project directory
    if _, err := p.BaseScope().ProjectDir(ctx); err != nil {
        return err // Returns helpful error message
    }
    // ... continue with pull logic
}
```

**Error Messages**:
- Suggests correct command based on directory type
- Example: "Not a project directory. Did you mean to run 'sync init' first?"

## Common Command Patterns

### Pattern 1: Simple Local Command

```go
// Example: local/validate/cmd.go
func Command(p dependencies.Provider) *cobra.Command {
    cmd := &cobra.Command{
        Use:   "validate",
        Short: helpmsg.Read(`local/validate/short`),
        Long:  helpmsg.Read(`local/validate/long`),
        RunE: func(cmd *cobra.Command, args []string) error {
            // 1. Require project directory
            _, err := p.BaseScope().ProjectDir(cmd.Context())
            if err != nil {
                return err
            }

            // 2. Bind flags
            f := Flags{}
            p.BaseScope().ConfigBinder().Bind(ctx, cmd.Flags(), args, &f)

            // 3. Get local scope (no auth needed)
            d, err := p.LocalCommandScope(ctx, f.StorageAPIHost)
            if err != nil {
                return err
            }

            // 4. Delegate to operation
            return validateOp.Run(ctx, options, d)
        },
    }

    configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())
    return cmd
}
```

### Pattern 2: Remote Command with Dialog

```go
// Example: remote/create/branch/cmd.go
func Command(p dependencies.Provider) *cobra.Command {
    cmd := &cobra.Command{
        Use:   "branch",
        Short: helpmsg.Read(`remote/create/branch/short`),
        RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
            // 1. Get remote scope (requires auth)
            f := Flags{}
            p.BaseScope().ConfigBinder().Bind(ctx, cmd.Flags(), args, &f)
            d, err := p.RemoteCommandScope(ctx, f.StorageAPIHost, f.StorageAPIToken)
            if err != nil {
                return err
            }

            // 2. Ask for options (interactive)
            options, err := AskCreateBranch(ctx, d.Dialogs(), d, f)
            if err != nil {
                return err
            }

            // 3. Send event (telemetry)
            defer d.EventSender().SendCmdEvent(ctx, d.Clock().Now(), &cmdErr, "remote-create-branch")

            // 4. Delegate to operation
            return createBranchOp.Run(ctx, options, d)
        },
    }

    configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())
    return cmd
}

// dialog.go
func AskCreateBranch(ctx, dialogs, deps, flags) (Options, error) {
    // Use flags if set, otherwise prompt
    name := flags.Name.Value
    if !flags.Name.IsSet() {
        name, _ = dialogs.Ask(&prompt.Question{
            Label:       "Branch name",
            Validator:   validate.BranchName,
        })
    }

    return Options{Name: name}, nil
}
```

### Pattern 3: Sync Command with State Management

```go
// Example: sync/push/cmd.go
func Command(p dependencies.Provider) *cobra.Command {
    cmd := &cobra.Command{
        Use:   "push",
        Short: helpmsg.Read(`sync/push/short`),
        RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
            // 1. Require project directory
            _, err := p.BaseScope().ProjectDir(cmd.Context())
            if err != nil {
                return err
            }

            // 2. Load project with both scopes
            prj, d, err := p.LocalProject(ctx, false, f.StorageAPIHost, f.StorageAPIToken, projectOpts)
            if err != nil {
                return err
            }

            // 3. Load state (local + remote)
            projectState, err := prj.LoadState(ctx, loadState.Options{
                LoadLocalState:  true,
                LoadRemoteState: true,
            }, d)
            if err != nil {
                return err
            }

            // 4. Send event
            defer d.EventSender().SendCmdEvent(ctx, d.Clock().Now(), &cmdErr, "sync-push")

            // 5. Delegate to operation
            return pushOp.Run(ctx, projectState, options, d)
        },
    }

    configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())
    return cmd
}
```

## State Management

### State Loading

**State Layers**:
- **LocalState**: Files on disk (`.keboola/` directory)
- **RemoteState**: From Storage API
- **State**: Combined view with both local and remote

**Loading Options** (`operation/state/load/`):
```go
type Options struct {
    LoadLocalState  bool
    LoadRemoteState bool
    LocalFilter     Filter
    RemoteFilter    Filter
    IgnoreNotFoundErr bool
    IgnoreInvalidLocalState bool
}
```

**State Structure**:
```go
type ObjectsContainer interface {
    All() []Object                    // All objects
    Branches() []*Branch              // Top-level branches
    Configs() []*Config               // All configs
    ConfigsFrom(branchKey) []*Config  // Configs in branch
    // ... more accessors
}
```

### Synchronization Flow

**Init** (`sync init`):
1. Validate empty directory
2. Authenticate with Storage API
3. Create manifest.json
4. Pull remote state → create local files
5. Generate .env files
6. Optionally generate CI workflows

**Pull** (`sync pull`):
1. Load local + remote state
2. Calculate diff (remote → local)
3. Display changes
4. Apply changes to local files
5. Validate result

**Push** (`sync push`):
1. Load local + remote state
2. Validate local state (schema + encryption)
3. Calculate diff (local → remote)
4. Display changes
5. Apply changes to remote API
6. Update local manifest

**Diff** (`sync diff`):
1. Load local + remote state
2. Calculate diff
3. Display changes (no modifications)

## Project Structure

**Project Directory** (after `sync init`):
```
project-dir/
├── .env.local                    # Local environment variables
├── .env                          # Committed environment variables
├── .keboola/                     # Keboola metadata
│   ├── manifest.json            # Project manifest (ID, API host, filters, etc.)
│   └── project/                 # Remote state cache
│       └── ...
├── main/                        # Main branch
│   ├── extractor-ex-component/  # Configuration directory
│   │   ├── config.json         # Config metadata
│   │   ├── description.md      # Config description
│   │   └── rows/               # Config rows
│   │       └── row-1/
│   │           └── config.json
│   └── transformation-keboola/
│       └── my-transformation/
│           ├── config.json
│           ├── blocks/         # Transformation blocks
│           └── codes/          # SQL/Python code
└── dev-branch/                  # Development branch
    └── ...
```

**Manifest File** (`.keboola/manifest.json`):
```json
{
  "version": 2,
  "project": {
    "id": 12345,
    "apiHost": "connection.keboola.com"
  },
  "allowedBranches": ["main", "dev-*"],
  "naming": {
    "branch": "{branch_name}",
    "config": "{component_type}-{component_id}/{config_name}",
    "row": "rows/{row_name}"
  },
  "templates": {
    "repositories": [
      {
        "type": "git",
        "name": "keboola",
        "url": "https://github.com/keboola/keboola-as-code-templates"
      }
    ]
  }
}
```

## Template Integration

**Template Types**:
1. **Project Template**: Creates entire project structure
2. **Config Template**: Creates single configuration
3. **Row Template**: Creates configuration row

**Template Structure**:
```
repository/
  ├── manifest.json              # Repository manifest
  └── template-name/
      └── v1/                    # Version directory
          ├── manifest.json      # Template manifest
          ├── inputs.json        # User input schema (jsonschema)
          ├── src/               # Template source files
          │   ├── main/
          │   │   └── config/
          │   │       └── config.json
          │   └── variables.json
          └── tests/             # Test configurations
```

**Template Commands**:
- `local template use` - Instantiate template
- `local template upgrade` - Upgrade instance to new version
- `local template rename` - Rename instance
- `local template delete` - Delete instance
- `template list` - List templates in repository
- `template describe` - Show template details

## Error Handling

**Error Types**:
- `DirNotFoundError` - Not in correct directory type
- `InvalidStateError` - Local state validation failed
- `MissingStorageAPITokenError` - Authentication required
- `ComponentSchemaError` - Config doesn't match component schema

**Error Enhancement** (`cmd/cmd.go`):
- Detects specific error types
- Provides user-friendly messages
- Suggests corrective actions
- Example: "Run 'kbc sync init' to initialize project"

**Logging**:
- Debug logs: Full stack traces → log file
- User output: Formatted messages → stderr
- Success messages → stdout

## Adding a New Command

**Step-by-step Guide**:

1. **Create command package**: `cmd/{group}/{command}/`
2. **Define command** (`cmd.go`):
   ```go
   func Command(p dependencies.Provider) *cobra.Command {
       cmd := &cobra.Command{
           Use:   "mycommand",
           Short: helpmsg.Read(`group/mycommand/short`),
           Long:  helpmsg.Read(`group/mycommand/long`),
           RunE:  func(cmd *cobra.Command, args []string) error {
               // Implementation
           },
       }
       configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())
       return cmd
   }
   ```

3. **Define flags**:
   ```go
   type Flags struct {
       MyFlag configmap.Value[string] `configKey:"my-flag" configShorthand:"f" configUsage:"description"`
   }

   func DefaultFlags() Flags {
       return Flags{
           MyFlag: configmap.NewValue("default"),
       }
   }
   ```

4. **Add dialog** (if interactive) (`dialog.go`):
   ```go
   func AskMyCommand(ctx, dialogs, deps, flags) (Options, error) {
       // Interactive prompts
   }
   ```

5. **Create operation**: `/pkg/lib/operation/{domain}/{command}/`
   ```go
   type Options struct {
       MyOption string
   }

   type Dependencies interface {
       Logger() log.Logger
       // Only needed dependencies
   }

   func Run(ctx context.Context, options Options, deps Dependencies) error {
       // Business logic
   }
   ```

6. **Add help messages**: `helpmsg/msg/{group}/{command}/`
   - `short.txt` - One-line description
   - `long.txt` - Detailed help

7. **Register command** in parent group (`cmd/{group}/cmd.go`):
   ```go
   cmd.AddCommand(mycommand.Command(p))
   ```

8. **Write tests**:
   - Unit tests for operation
   - Integration tests for command

## Testing Commands

**Operation Tests**:
```go
func TestMyOperation(t *testing.T) {
    // Mock dependencies
    deps := &mockDeps{
        logger: log.NewNopLogger(),
    }

    // Run operation
    err := Run(ctx, options, deps)

    // Assert
    assert.NoError(t, err)
}
```

**Dialog Tests**:
```go
func TestDialog(t *testing.T) {
    // Create virtual terminal
    d := dialog.NewForTest(t, true) // interactive=true
    d.Ask("question", "answer")     // Pre-program response

    // Run dialog
    result, _ := AskMyCommand(ctx, d, deps, flags)

    // Assert
    assert.Equal(t, expected, result)
}
```

**Command Integration Tests**:
- Located in `/test/cli/`
- Use snapshot testing for output
- Mock HTTP client for API calls

## Best Practices

1. **Keep commands thin** - Delegate to operations
2. **Use appropriate scope** - Don't request RemoteScope if BaseScope is enough
3. **Support non-interactive mode** - Always check `flags.IsSet()` before prompting
4. **Validate early** - Use FsInfo to check directory type
5. **Send events** - Track command success/failure for telemetry
6. **Provide helpful errors** - Include suggestions for fixing issues
7. **Write help messages** - Short description + detailed long description
8. **Test operations separately** - Don't test CLI and business logic together
9. **Follow flag naming** - Use kebab-case, ENV uses SCREAMING_SNAKE_CASE
10. **Document dialogs** - Explain what each prompt does in comments

## Common Gotchas

1. **Forgetting to propagate context** - Use `util.PropagateContext(cmd)`
2. **Mixing business logic in commands** - Keep it in operations
3. **Not handling non-interactive mode** - Command fails in CI
4. **Wrong scope level** - Requesting RemoteScope when LocalScope is enough
5. **Not validating directory type** - Command fails with confusing error
6. **Hardcoding strings** - Use helpmsg for all user-facing text
7. **Forgetting event tracking** - Can't measure command usage
8. **Not checking flag.IsSet()** - Defaults override user's choice
9. **Ignoring errors** - Always check and handle errors properly
10. **Testing commands directly** - Test operations, mock dialogs

## Key Files to Reference

- `cmd/cmd.go` - Root command setup, error handling
- `cmd/sync/init/cmd.go` - Example of complete command with dialog
- `dependencies/provider.go` - Dependency injection entry point
- `dependencies/fsinfo.go` - Directory validation patterns
- `prompt/prompt.go` - Prompt interface definition
- `dialog/dialog.go` - Dialog wrapper with helpers
- `/pkg/lib/operation/project/sync/` - Sync operations implementation
