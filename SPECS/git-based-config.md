# Git-Based Configuration Management

This document describes the enhanced developer experience features for managing Keboola configurations through git, combining git-branching workflows with a developer-friendly file structure.

## Overview

The goal is to enable a true GitOps workflow where:
1. **Git branches map to Keboola branches** - Work on feature branches syncs to isolated Keboola dev branches
2. **File formats are developer-friendly** - SQL/Python code in standard files, YAML configs, standard tooling
3. **Code review works naturally** - Changes are reviewable as normal diffs
4. **Agentic use is safe and aligned** - MCP tools operate in the same branch context as the CLI and are complementary to file system operations

---

## Git-Branching Mode

### The Problem

Without git-branching, developers must manually coordinate which Keboola branch they're syncing to. This leads to:
- Accidental pushes to production
- Confusion about which dev branch to use
- Manual `KBC_BRANCH_ID` environment variable management

### The Solution

Enable git-branching mode during project initialization:

```bash
kbc init --git-branching
```

This creates a mapping system where each git branch is linked to a Keboola branch:
- `main` → Keboola production branch
- `feature/*` → Keboola dev branches (created on demand)

### How It Works

**Branch Mapping File** (`.keboola/branch-mapping.json`, committed to git):
```json
{
  "version": 1,
  "mappings": {
    "main": { "id": null, "name": "Main" },
    "feature/auth": { "id": "972851", "name": "feature/auth" }
  }
}
```

**Workflow:**
```bash
# Start feature work
git checkout -b feature/new-etl
kbc branch link                    # Creates Keboola branch, saves mapping

# Sync operations auto-target the linked branch
kbc sync pull                      # Pulls from feature/new-etl Keboola branch
kbc sync push                      # Pushes to feature/new-etl Keboola branch

# Teammates get the mapping via git
git pull                           # Gets branch-mapping.json
kbc sync pull                      # Works immediately, no setup needed
```

**Enforcement:** Sync commands (`push`, `pull`, `diff`) are blocked until the branch is linked, preventing accidental production changes.

### Branch Commands

| Command | Description |
|---------|-------------|
| `kbc branch link` | Link current git branch to a Keboola branch (creates if needed) |
| `kbc branch unlink` | Remove mapping for current branch |
| `kbc branch list` | Show all mappings |
| `kbc branch status` | Show current branch mapping |

---

## Agentic Use (MCP)

Agentic use extends git-branching with a local MCP proxy so AI agents can work safely and consistently in the same branch context as the CLI.

### What It Generates

When initializing git-branching mode, the CLI generates:
- `CLAUDE.md` - agent instructions for this project
- `AGENTS.md` - cross-agent instructions
- `.mcp.json` - MCP server auto-configuration

These are committed to keep agent workflows consistent across tools.

### Local MCP Proxy (Branch-Scoped)

Agentic mode is used with a lightweight local MCP server wrapper (created as part of the MVP) that proxies to:

```
mcp-agent.x.keboola.com/mcp
```

The wrapper resolves the current git branch mapping and injects the `X-Branch-Id` header on each request. It is intended to be used only locally and only in combination with git-branching mode. The proxy forces MCP operations to target a linked Keboola branch, preventing accidental access to the default/production branch.

### Why It’s Beneficial

The key benefit is combining local CLI access with MCP tools while keeping branch context aligned:
- **CLI** manages the filesystem representation (edits, validation, git history)
- **MCP tools** operate live (component schemas, tables, jobs, previews)
- **Branch alignment** is automatic and shared across both, avoiding drift

Typical flow:
1. Use MCP tools to discover components or run jobs.
2. Use the CLI to create/edit configs locally.
3. Pull after MCP changes (`kbc sync pull`) to keep the repo in sync.

This hybrid workflow keeps agent automation safe, reviewable, and branch-isolated.

---

## Developer-Friendly File Structure

### The Problem

The current Keboola file structure is API-centric, not developer-centric:
- Transformation code split across `blocks/` directories with `code.sql` files
- Configuration in `config.json` + `meta.json` + `description.md`
- Python dependencies embedded in JSON
- Orchestrations as deep directory trees

This makes code review difficult and prevents using standard development tools.

### The Solution

Restructure files for developer workflows while maintaining full compatibility:

### 1. Consolidated Transformation Code

**Before:**
```
transformation/
└── my-transform/
    └── blocks/
        ├── 001-setup/
        │   └── 001-init/
        │       └── code.sql
        └── 002-main/
            └── 001-process/
                └── code.sql
```

**After:**
```
transformation/
└── my-transform/
    ├── _config.yml
    └── transform.sql      # All SQL in one file, block markers preserved
```

The `transform.sql` file uses markers to preserve block structure:
```sql
/* ===== BLOCK: 001-setup ===== */
/* ===== CODE: 001-init ===== */
CREATE TABLE staging AS SELECT ...;

/* ===== BLOCK: 002-main ===== */
/* ===== CODE: 001-process ===== */
INSERT INTO output SELECT ...;
```

### 2. Unified YAML Configuration

**Before:** Three files (`config.json`, `meta.json`, `description.md`)

**After:** Single `_config.yml`:
```yaml
name: "My Transformation"
description: |
  Multi-line description
  with full markdown support

parameters:
  query: "SELECT * FROM input"

storage:
  input:
    tables:
      - source: "in.c-main.users"
        destination: "users"
```

### 3. Python Dependencies in pyproject.toml

**Before:** Dependencies in `config.json`:
```json
{
  "parameters": {
    "packages": ["pandas==2.0.0", "requests"]
  }
}
```

**After:** Standard `pyproject.toml`:
```toml
[project]
dependencies = [
    "pandas==2.0.0",
    "requests",
]
```

Benefits: IDE support, pip/uv compatibility, dependency locking.

### 4. Local Development with `kbc local data`

Download input tables locally to develop and test transformations:

```bash
# Download input tables from Keboola to local files
kbc local data transformation/my-transform

# Tables are saved to the transformation's in/tables directory
ls transformation/my-transform/in/tables/
# users.csv  orders.csv

# Now develop your transformation with real data
# Run Python locally using standard tools
cd transformation/my-transform  
uv run python transform.py
```

**Why it's useful:**
- Develop with real data samples locally
- Use IDE debugging and breakpoints
- Fast iteration without Keboola job queues
- Validate transformations before push
- This works for SQL, Python transformations and Custom Python components

### 5. Orchestration in _config.yml

**Before:** Deep directory structure:
```
orchestration/
└── my-pipeline/
    └── phases/
        ├── 001-extract/
        │   └── tasks/
        │       └── 001-api-call/
        └── 002-transform/
```

**After:** Single `_config.yml` with metadata, schedules, and phases:
```yaml
name: "My Pipeline"
description: |
  Daily sync and transform pipeline.

schedules:
  - name: Daily morning run
    cron: "0 6 * * *"
    timezone: Europe/Prague
    enabled: true

phases:
  - name: "Extract"
    tasks:
      - component: "keboola.ex-generic-v2"
        config: "extractor/keboola.ex-generic-v2/api-extractor"

  - name: "Transform"
    depends_on: ["Extract"]
    tasks:
      - component: "keboola.snowflake-transformation"
        config: "transformation/keboola.snowflake-transformation/main-transform"
```

---

## Why These Changes Matter

| Change | Developer Benefit |
|--------|-------------------|
| Git-branching | Safe parallel development, no manual branch coordination |
| Consolidated SQL/Python | Full file visible in PR diffs, IDE support works |
| YAML configs | Human-readable, standard format, better diff output |
| pyproject.toml | Standard Python tooling, IDE autocomplete for packages |
| _config.yml (orchestration) | Entire orchestration visible at once, easy to modify |

### Code Review Example

**Before:** A transformation change might touch 5+ files across nested directories, making review difficult.

**After:** Change is visible in one `transform.sql` file diff:
```diff
  /* ===== BLOCK: 002-main ===== */
  /* ===== CODE: 001-process ===== */
- INSERT INTO output SELECT id, name FROM users;
+ INSERT INTO output SELECT id, name, email FROM users WHERE active = true;
```

---

## CI/CD with GitHub Actions

Automate sync operations with GitHub Actions for a complete GitOps workflow.

### Available Workflows

| Workflow | Trigger | Description |
|----------|---------|-------------|
| **Push** | On push to branch | Syncs local changes to Keboola when code is pushed |
| **Pull** | Scheduled/manual | Pulls Keboola changes back to git (captures UI edits) | 

### How It Works

1. **Push on commit** - When you push to a git branch, the workflow automatically:
   - Looks up the Keboola branch ID from the mapping
   - Creates a new Keboola branch if needed
   - Runs `kbc push` to sync changes

2. **Scheduled pull** - A cron job periodically:
   - Pulls changes from Keboola (captures edits made in UI)
   - Commits them back to git
   - Keeps git and Keboola in sync

3. **Concurrency control** - Push cancels pending pulls to avoid conflicts

### Setup

1. Add `KBC_STORAGE_API_TOKEN` to repository secrets
2. Copy the workflow files from the example repository
3. Adjust triggers (branch filters, schedule) as needed

### Example Repository

See [keboola/CLI-git-based-branch-mgmt](https://github.com/keboola/CLI-git-based-branch-mgmt) for complete GitHub Actions workflows and reusable actions.

---

## Implementation Status

- [x] Git-branching mode (`kbc init --git-branching`)
- [x] Branch commands (`link`, `unlink`, `list`, `status`)
- [x] Sync enforcement (blocks unlinked branches)
- [x] Block-code consolidation (`transform.sql`)
- [x] Orchestration pipeline format in `_config.yml`
- [x] Inline schedules in orchestration config
- [x] Unified YAML configuration (`_config.yml`)
- [x] Python dependencies (`pyproject.toml`)
- [x] Local data command (`kbc local data`)
- [x] Migration tooling (old → new format)

---

## Related Documentation

- [Git-Branching Specification](git-branching/SPECS.md)
- [Developer-Friendly Structure Specification](dev-friendly-structure/SPECS.md)
- [CLI Architecture](../internal/pkg/service/cli/CLI_CONTEXT.md)
