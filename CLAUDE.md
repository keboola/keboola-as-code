# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Important: Read PROJECT_CONTEXT.md First

**Before working on this codebase, read [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md)** for:
- Project overview and directory structure
- Local development setup (Docker and non-Docker)
- Service URLs, ports, and deployment information
- CI/CD pipeline and infrastructure requirements

This file (CLAUDE.md) contains **additional** architecture patterns, coding conventions, and development practices specific to this codebase that are not covered in PROJECT_CONTEXT.md.

## Architecture Patterns

### Command/Operation Pattern
All business logic is encapsulated in reusable operations under `/pkg/lib/operation/`. Operations follow this pattern:

```go
type Dependencies interface {
    Logger() log.Logger
    // ... only needed dependencies
}

func Run(ctx context.Context, deps Dependencies, options Options) error {
    // Business logic here
}
```

Keep CLI commands thin - they should only parse arguments and call operations.

### Dependency Injection Hierarchy
The DI system uses a scope hierarchy:
- `BaseScope` - Basic dependencies (logger, telemetry, clock, HTTP client)
- `PublicScope` - Unauthenticated access
- `ProjectScope` - Authenticated with Storage API token
- `EtcdClientScope` - etcd operations
- `TaskScope`, `DistributionScope` - Cluster coordination

### etcd Operations Framework
Custom abstraction over etcd (`internal/pkg/service/common/etcdop/`) provides:
- Type-safe operations with serialization/deserialization
- Watch operations with automatic restart on failures
- Prefix-based namespacing for multi-tenancy

### API Design with Goa
APIs are designed using Goa DSL in `/api/` directory. After modifying the design:
- Run `task generate-<service>-api` to regenerate server code, OpenAPI specs, and types
- Never hand-edit generated code

## Docker Development

If Go is not available locally, use Docker Compose to run all commands:

```bash
# Start a shell in the dev container
docker-compose run --rm -u "$UID:$GID" --service-ports dev bash

# Then run commands inside the container
task lint
task tests
```

## Common Development Commands

### Building (Local Development)
- `task build-local` - Build CLI for current platform (output: `./target`)
- `task build-templates-api` - Build Templates API service
- `task build-stream-service` - Build Stream service
- `task build-apps-proxy` - Build Apps Proxy service

**CI/CD only** (avoid locally - slow):
- `task build` - Build CLI for all architectures (use only in CI/CD)
- `task ci` - Run linting and all tests (use only in CI/CD)

### Testing (Run Specific Tests)

**Run specific test** (recommended approach for local development):
```bash
# Run specific test by name
go test -race -v ./path/to/pkg... -run TestName
go test -race -v ./path/to/pkg... -run TestName/SubTest

# Run specific E2E test
task e2e -- test/cli/path/to/test
```

**Verbose testing** (shows HTTP requests, ENVs, etcd operations):
```bash
TEST_VERBOSE=true go test -race -v -p 1 ./path/to/pkg... -run TestName/SubTest
TEST_HTTP_CLIENT_VERBOSE=true TEST_VERBOSE=true go test -race -v -p 1 ./path/to/pkg...
ETCD_VERBOSE=true TEST_VERBOSE=true go test -race -v -p 1 ./path/to/pkg...
```

### Linting & Formatting
- `task lint` - Run linter
- `task fix` - Auto-fix linting issues and run `go mod vendor`
- `task mod` - Update go modules and vendor directory

### Running Services Locally
All services use Docker Compose for development:

1. Start dev container: `docker compose run --rm -u "$UID:$GID" --service-ports dev bash`
2. Set environment variables:
   - Templates API: `export TEMPLATES_STORAGE_API_HOST=connection.keboola.com`
   - Stream Service: `export STREAM_STORAGE_API_HOST=connection.keboola.com`
3. Run service:
   - `task run-templates-api` - API at http://localhost:8000/
   - `task run-stream-service` - API at http://localhost:8001/
   - `task run-apps-proxy` - Apps Proxy service

Services auto-reload on code changes using Air.

### API Documentation
- `task godoc` - Start Go documentation server at http://localhost:6060/pkg/github.com/keboola/keboola-as-code/?m=all
- OpenAPI documentation available at `localhost:<port>/v1/documentation` when service is running

### Code Generation
- `task generate-templates-api` - Regenerate Templates API from Goa design
- `task generate-stream-api` - Regenerate Stream API from Goa design
- `task generate-appsproxy-api` - Regenerate Apps Proxy API from Goa design
- `task generate-model` - Generate domain models
- `task generate-protobuf` - Generate protobuf code

## Code Style & Patterns

### Module Organization
- Keep modules under 500 lines (excluding tests)
- Follow KISS principle - clear, straightforward code
- Follow DRY principle - extract common functionality

### Avoid Deep Nesting
- **Maximum 2 levels of nesting** - Extract helper functions for deeper loops
- **Use early returns** - Invert conditions and return/continue early to reduce indentation
- **Extract inner loops** - When you have nested `for` loops, extract the inner loop to a separate function
- **Flatten conditionals** - Replace `if { if { if { }}}` with guard clauses and early returns

Example - instead of:
```go
for _, item := range items {
    if item != nil {
        for _, sub := range item.Children {
            if sub.Valid {
                process(sub)
            }
        }
    }
}
```
Use:
```go
for _, item := range items {
    if item == nil {
        continue
    }
    processChildren(item.Children)
}

func processChildren(children []Child) {
    for _, sub := range children {
        if !sub.Valid {
            continue
        }
        process(sub)
    }
}
```

### Prohibited Patterns
- **Global variables** - Use dependency injection
- **init() functions** - Use explicit initialization
- **Debug prints** (`fmt.Print*`, `print`, `println`) - Use logger instead
- **Direct "os" package filesystem operations** - Use `internal/pkg/filesystem` instead
- **Direct "filepath" package usage** - Use `internal/pkg/filesystem` instead
- **httpmock singleton** - Use `client.Transport = httpmock.NewMockTransport()`
- **OS ENV singleton** (`os.Setenv`) - Use `env.Map` instead
- **Direct os.Stdout/os.Stderr** - Use dependencies instead
- **fmt.Errorf** - Use `errors.Errorf` for stack traces (from `internal/pkg/utils/errors`)
- **"gonanoid" package** - Use `internal/pkg/idgenerator` instead
- **Direct "errors" package** - Use `internal/pkg/utils/errors`
- **Direct "zap" logger** - Use `internal/pkg/log` package
- **Naked returns**
- **Underscores in package names**
- **Deprecated functions** - Remove deprecated functions instead of keeping them; don't mark as deprecated if still needed
- **Variables only used for logging** - Don't create variables solely for debug/info logging; inline or remove them
- **Pseudo-version SDK dependencies** - Use tagged versions (e.g., `v2.12.0`) instead of pseudo-versions (e.g., `v2.9.1-0.20260109014800-d596b2c092e2`)

### Go Naming Conventions
- **No "Get" prefix for getters** - Use `Type()` not `GetType()`, `Name()` not `GetName()`, `Source()` not `GetSource()`
- **Use "Get" only when it fetches external data** - e.g., `GetFromAPI()`, `GetFromDatabase()`

### Function Return Values
- **Wrap multiple return values** - When returning more than 3 values, wrap them in a struct
- **Return errors, don't silently continue** - When an operation fails, return the error instead of continuing with empty/default values

Example - instead of:
```go
func fetchData() (a, b, c, d Type, err error)
```
Use:
```go
type fetchResult struct {
    A, B, C, D Type
}
func fetchData() (fetchResult, error)
```

### String Building
- **Use `strings.Builder` for concatenation** - More efficient than `fmt.Sprintf` or `+` for building strings

Example:
```go
func buildUID(prefix, name string) string {
    var b strings.Builder
    b.WriteString(prefix)
    b.WriteByte(':')
    b.WriteString(name)
    return b.String()
}
```

### Required Patterns
- **Context handling**: Pass context as first parameter; respect cancellation; never store in structs
- **Error wrapping**: Use error wrapping with stack traces; custom error types for domain logic
- **Dependency management**: Constructor-based DI; interface segregation (small interfaces)
- **Observability**: Structured logging; OpenTelemetry integration; metrics for critical paths
- **Early returns**: Prefer early `return` / `continue` to reduce nesting
- **Default-first assignment**: Set default value first, then override if present. Avoid resetting nil values after assignment.

Example - instead of:
```go
value := someMap[key]
if value == nil {
    value = defaultValue
}
```
Use:
```go
value := defaultValue
if v, ok := someMap[key]; ok {
    value = v
}
```

Similarly for struct fields:
```go
// Good: set default, override if not nil
items := []string{}
if data.Items != nil {
    items = data.Items
}

// Bad: assign then reset if nil
items := data.Items
if items == nil {
    items = []string{}
}
```

### Commit Practices
- **Make small, focused commits** - One logical change per commit
- **Don't bundle multiple tasks** into a single commit; split them for easier review and rebasing
- **Example**: If implementing column metadata, transformation code output, and component configs, create 3 separate commits rather than one large commit

### Testing
- Test files use `*_test.go` suffix and are located next to implementation
- Use `testify/assert` for assertions
- Table-driven tests preferred
- Coverage target: 80%
- E2E tests use real etcd and mocked HTTP clients
- Test projects configured via `.env` file pointing to `projects.json`
- **Use `t.Context()` instead of `context.Background()`** - Go 1.21+ provides test context via `t.Context()` which is automatically cancelled when the test ends

## State Management Architecture

The CLI implements bidirectional sync between local directory and Keboola Storage API:

- **Remote State**: Fetched from Keboola Storage API
- **Local State**: Files in directories with manifest files for metadata
- **Diff/Merge**: Calculates changes and applies them bidirectionally

## Testing Setup

Create `.env` file for E2E tests:
```
TEST_KBC_TMP_DIR=/tmp
TEST_KBC_PROJECTS_FILE=~/keboola-as-code/projects.json
```

Create `projects.json` (gitignored):
```json
[
  {
    "host": "connection.keboola.com",
    "project": 1234,
    "stagingStorage": "s3",
    "backend": "snowflake",
    "token": "<token>",
    "legacyTransformation": "false"
  }
]
```

Staging storage can be `s3`, `abs`, or `gcs`.

## Where to Add New Features

- **CLI command?** → `/internal/pkg/service/cli/cmd/` + workflow in `/pkg/lib/operation/`
  - See [CLI_CONTEXT.md](internal/pkg/service/cli/CLI_CONTEXT.md) for detailed CLI architecture and patterns
- **API endpoint?** → Modify `/api/<service>/design.go`, run `task generate-<service>-api`
- **Business logic/implementation?** → `/internal/pkg/` (the actual heavy lifting)
  - `/pkg/lib/operation/` contains workflow orchestration and options, delegates to `internal/pkg/`
- **Distributed coordination?** → Use etcdop framework in `/internal/pkg/service/common/etcdop/`
- **New dependency?** → Update appropriate scope in `/internal/pkg/service/<service>/dependencies/`

## Service-Specific Documentation

- **CLI Service**: [internal/pkg/service/cli/CLI_CONTEXT.md](internal/pkg/service/cli/CLI_CONTEXT.md) - Comprehensive guide to CLI architecture, command patterns, dependency injection, dialogs, and state management
