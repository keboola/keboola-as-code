# Keboola CLI Git-Branching Mode Specification

**Version:** 2.0
**Date:** January 2026
**Status:** Draft

---

## 1. Overview

The `--git-branching` flag for `kbc init` enables a DevOps workflow where git branches are automatically mapped to Keboola branches. This mode:

- Establishes git-to-Keboola branch mapping workflow
- Provides CLI commands for branch linking (`kbc branch link/unlink/list`)
- Blocks sync commands until branch is explicitly linked
- Generates agent instruction files for AI coding assistants
- Auto-configures MCP server for enhanced AI agent support

### Key Concept: Shared Branch Mappings

Unlike per-developer mappings, this design stores mappings in `.keboola/branch-mapping.json` which is **committed to git**. When a developer creates a feature branch and links it to a Keboola branch, that mapping is shared with all team members via version control.

---

## 2. Command Reference

### 2.1 Initialization

```bash
kbc init --git-branching [options]
```

| Option | Description | Default |
|--------|-------------|---------|
| `--git-branching` | Enable git-branching DevOps mode | `false` |
| `--default-branch` | Git default branch name | Auto-detected |
| `--mcp-package` | MCP server npm package name | `@keboola/mcp-server` |
| `--force` | Overwrite existing agent files | `false` |

### 2.2 Branch Management Commands

#### `kbc branch link`

Links the current git branch to a Keboola development branch.

```bash
kbc branch link [options]
```

| Option | Description |
|--------|-------------|
| `--branch-id=<id>` | Link to existing Keboola branch by ID |
| `--branch-name=<name>` | Link to existing branch by name, or create with this name |

**Default behavior** (no options):
1. Get current git branch name
2. If on default branch (main/master): **Error** - default branch is auto-linked during init
3. If already linked: Show existing mapping
4. Otherwise:
   - Search for existing Keboola branch with same name
   - If found: Link to it
   - If not found: Create new Keboola dev branch, then link

**Examples:**
```bash
# On feature/auth branch - creates Keboola branch "feature/auth" and links
$ kbc branch link
✓ Created Keboola branch "feature/auth" (ID: 972851)
✓ Linked git branch "feature/auth" → Keboola branch 972851

# Link to existing branch by ID
$ kbc branch link --branch-id=972851
✓ Linked git branch "feature/auth" → Keboola branch 972851

# Link to branch by name (creates if doesn't exist)
$ kbc branch link --branch-name="dev-environment"
✓ Linked git branch "feature/auth" → Keboola branch "dev-environment" (ID: 983421)

# Error on default branch
$ kbc branch link
Error: Cannot link the default branch.

   The default branch "main" is automatically linked to Keboola production
   during 'kbc init --git-branching'. This mapping cannot be changed.

   Switch to a feature branch to create a new mapping:
   git checkout -b feature/my-feature
   kbc branch link
```

#### `kbc branch unlink`

Removes the mapping for the current git branch.

```bash
kbc branch unlink
```

**Behavior:**
- Removes mapping from `branch-mapping.json`
- Does NOT delete the Keboola branch
- **Cannot unlink default branch** - errors if on main/master

**Examples:**
```bash
$ kbc branch unlink
✓ Unlinked git branch "feature/auth" from Keboola branch 972851

Note: The Keboola branch still exists. Delete it manually if needed:
  kbc remote delete branch --branch-id=972851

# Error on default branch
$ kbc branch unlink
Error: Cannot unlink the default branch.

   The default branch "main" is permanently linked to Keboola production.
   This mapping cannot be removed.
```

#### `kbc branch list`

Lists all git-to-Keboola branch mappings.

```bash
kbc branch list
```

**Example:**
```bash
$ kbc branch list
Git Branch         Keboola Branch ID    Keboola Branch Name
─────────────────────────────────────────────────────────────
main               (production)         Main
feature/auth     → 972851               feature/auth
feature/data     → 983421               feature/data
* feature/auth (current)
```

#### `kbc branch status`

Shows the mapping status for the current git branch.

```bash
kbc branch status
```

**Example (mapped):**
```bash
$ kbc branch status
Git branch:      feature/auth
Keboola branch:  972851 (feature/auth)
Status:          ✓ Linked

Ready to use sync commands (push, pull, diff).
```

**Example (not mapped):**
```bash
$ kbc branch status
Git branch:      feature/new-thing
Keboola branch:  (none)
Status:          ✗ Not linked

Run 'kbc branch link' to create and link a Keboola branch.
```

---

## 3. Prerequisites

### Required

1. **Git repository initialized** - `.git/` directory must exist
2. **Current branch is default branch** - Must run init from main/master (or specify `--default-branch`)
3. **Keboola project context** - Valid Storage API token and project URL

### Validation Flow

```
1. Check .git/ directory exists
   └─ If missing → Error: GIT_NOT_INITIALIZED

2. Check git remote is configured (warning only)
   └─ If missing → Warning: GIT_NO_REMOTE

3. Determine and validate default branch:
   a. If --default-branch flag provided → use that value
   b. Else try to detect from git remote
   c. Else if current branch is "main" or "master" → use current branch
   d. Else → Error: DEFAULT_BRANCH_UNKNOWN

4. Verify current branch matches default branch
   └─ If not on default branch → Error: MUST_RUN_FROM_DEFAULT_BRANCH

5. Validate Keboola credentials (existing kbc init behavior)

6. Auto-link default branch to Keboola production
   └─ Add mapping: { "main": { "id": null, "name": "Main" } }
```

---

## 4. File Structure

### 4.1 Generated/Modified Files

```
project-root/
├── CLAUDE.md                         # Claude Code instructions
├── AGENTS.md                         # Cross-agent instructions
├── .mcp.json                         # MCP server configuration
├── .gitignore                        # Updated (append if exists)
└── .keboola/
    ├── manifest.json                 # Updated with gitBranching config
    └── branch-mapping.json           # Git-to-Keboola mappings (committed)
```

### 4.2 File Purposes

| File | Purpose | Git Status |
|------|---------|------------|
| `CLAUDE.md` | Primary agent instructions | Committed |
| `AGENTS.md` | Cross-agent compatibility | Committed |
| `.mcp.json` | MCP server auto-configuration | Committed |
| `.keboola/manifest.json` | Project config with gitBranching section | Committed |
| `.keboola/branch-mapping.json` | Git-to-Keboola branch mappings | **Committed** |

---

## 5. Configuration Files

### 5.1 Manifest Configuration

Updated `.keboola/manifest.json`:

```json
{
  "version": 2,
  "project": {
    "id": 12345,
    "apiHost": "connection.keboola.com"
  },
  "allowTargetEnv": true,
  "gitBranching": {
    "enabled": true,
    "defaultBranch": "main"
  },
  "branches": [...]
}
```

**Note:** `allowTargetEnv: true` is required for git-branching to work, as it enables `KBC_BRANCH_ID` environment variable override.

### 5.2 Branch Mapping File

`.keboola/branch-mapping.json`:

```json
{
  "version": 1,
  "mappings": {
    "main": {
      "id": null,
      "name": "Main"
    },
    "feature/auth": {
      "id": "972851",
      "name": "feature/auth"
    },
    "feature/data-pipeline": {
      "id": "983421",
      "name": "feature/data-pipeline"
    }
  }
}
```

**Format:**
- `version`: Schema version for future compatibility
- `mappings`: Object keyed by git branch name
  - `id`: Keboola branch ID (string) or `null` for production
  - `name`: Keboola branch name (for display purposes)

### 5.3 MCP Configuration

`.mcp.json`:

```json
{
  "mcpServers": {
    "keboola": {
      "command": "uvx",
      "args": [
        "{{MCP_PACKAGE}}",
        "--working-dir", "{{WORKING_DIR}}",
        "--git-default-branch", "{{DEFAULT_BRANCH}}"
      ],
      "env": {
        "KBC_STORAGE_API_TOKEN": "${KBC_STORAGE_API_TOKEN}",
        "KBC_STORAGE_API_HOST": "${KBC_STORAGE_API_HOST}"
      }
    }
  }
}
```

**Template Variables** (replaced during `kbc init --git-branching`):

| Variable | Source | Example |
|----------|--------|---------|
| `{{MCP_PACKAGE}}` | `--mcp-package` flag or default | `keboola-mcp-server` |
| `{{WORKING_DIR}}` | Project root directory | `/path/to/project` or `.` |
| `{{DEFAULT_BRANCH}}` | Detected/specified default branch | `main` |

**Environment Variables** (same as CLI):

| Variable | Description |
|----------|-------------|
| `${KBC_STORAGE_API_TOKEN}` | User's Storage API token |
| `${KBC_STORAGE_API_HOST}` | Keboola stack host (e.g., `connection.keboola.com`) |

**MCP Server Arguments:**

| Argument | Required | Description |
|----------|----------|-------------|
| `--working-dir` | Yes | Path to the CLI project root directory |
| `--git-default-branch` | Yes | Name of the git default branch (e.g., `main`) |

The MCP server uses these arguments to:
- Resolve the current git branch from the working directory
- Look up branch mappings from `.keboola/branch-mapping.json`
- Determine if current branch is the default (production) branch

---

## 6. Sync Command Enforcement

When `gitBranching.enabled = true` in manifest, all sync commands enforce branch mapping:

### 6.1 Blocked Commands

The following commands require a linked branch:
- `kbc sync push`
- `kbc sync pull`
- `kbc sync diff`

### 6.2 Error When Not Linked

```bash
$ kbc sync push

Error: Git branch not linked to Keboola branch.

   Current git branch: feature/new-thing

   This project uses git-branching mode, which requires each git branch
   to be explicitly linked to a Keboola branch before sync operations.

   To fix:
   kbc branch link              # Create and link a new Keboola branch
   kbc branch link --branch-id=<id>  # Link to existing branch

```

Exit code: `1`

### 6.3 Successful Execution

When branch is linked, commands proceed normally with automatic branch context:

```bash
$ kbc sync push

Using Keboola branch: feature/auth (ID: 972851)

Plan for "push" operation:
  ...
```

---

## 7. CLI Output Examples

### 7.1 Initialization Success

**Note:** Must be run from the default git branch (main/master).

```
$ kbc init --git-branching

Initializing Keboola project with git-branching mode...

✓ Git repository detected
✓ Default branch: main (auto-detected from remote)
✓ Linked main → Keboola production (auto-linked)
✓ Updated .keboola/manifest.json (gitBranching enabled)
✓ Created .keboola/branch-mapping.json
✓ Created CLAUDE.md (agent instructions)
✓ Created AGENTS.md (cross-agent instructions)
✓ Created .mcp.json (MCP server configuration)
✓ Updated .gitignore

🔀 Git-branching mode enabled.

   Git branches are mapped to Keboola branches:
   - main (git) → Keboola production (auto-linked, cannot be changed)
   - other branches → use 'kbc branch link' to create mapping

   Next steps:
   1. Ensure environment variables are set:
      export KBC_STORAGE_API_TOKEN="your-token"
      export KBC_STORAGE_API_HOST="connection.keboola.com"

   2. When working on a feature branch:
      git checkout -b feature/my-feature
      kbc branch link    # Creates Keboola branch and links it

   3. Sync your changes:
      kbc sync pull      # Pull from Keboola
      kbc sync push      # Push to Keboola

```

### 7.2 Error: Git Not Initialized

```
$ kbc init --git-branching

Error: Git repository not found.

   The --git-branching mode requires an initialized git repository.

   To fix:
   1. Initialize git: git init
   2. (Optional) Add remote: git remote add origin <url>
   3. Run: kbc init --git-branching

```

### 7.3 Error: Cannot Determine Default Branch

```
$ kbc init --git-branching

Error: Cannot determine default branch.

   Current branch: feature/my-feature
   No remote configured to detect default branch.

   To fix, either:
   1. Run from your default branch: git checkout main && kbc init --git-branching
   2. Specify explicitly: kbc init --git-branching --default-branch=main

```

### 7.4 Error: Not on Default Branch

```
$ kbc init --git-branching

Error: Must run from the default branch.

   Current branch: feature/my-feature
   Default branch: main

   The --git-branching init must be run from the default branch to
   auto-link it to Keboola production.

   To fix:
   git checkout main
   kbc init --git-branching

```

### 7.5 Error: Files Already Exist

```
$ kbc init --git-branching

Error: Agent configuration files already exist.

   Found existing files:
   - CLAUDE.md
   - AGENTS.md

   To overwrite, run: kbc init --git-branching --force

```

---

## 8. Workflow Examples

### 8.1 Initial Setup

```bash
# Clone repository
git clone https://github.com/myorg/keboola-project.git
cd keboola-project

# Initialize with git-branching
export KBC_STORAGE_API_TOKEN="your-token"
export KBC_STORAGE_API_HOST="connection.keboola.com"
kbc init --git-branching

# Main branch is auto-mapped to production
kbc branch status
# Git branch: main
# Keboola branch: (production)
# Status: ✓ Linked
```

### 8.2 Feature Branch Workflow

```bash
# Create feature branch
git checkout -b feature/new-etl

# Link to Keboola (creates new dev branch)
kbc branch link
# ✓ Created Keboola branch "feature/new-etl" (ID: 994521)
# ✓ Linked git branch "feature/new-etl" → Keboola branch 994521

# Commit the mapping so teammates can use it
git add .keboola/branch-mapping.json
git commit -m "Link feature/new-etl to Keboola branch"

# Now sync operations work
kbc sync pull   # Pull from your dev branch
# ... make changes ...
kbc sync push   # Push to your dev branch
```

### 8.3 Teammate Joining Feature Branch

```bash
# Teammate pulls the branch
git fetch origin
git checkout feature/new-etl

# Mapping already exists from teammate's commit
kbc branch status
# Git branch: feature/new-etl
# Keboola branch: 994521 (feature/new-etl)
# Status: ✓ Linked

# Ready to work immediately
kbc sync pull
```

---

## 9. Integration with MCP Server

The MCP server continues to provide enhanced AI agent support. When git-branching mode is enabled:

1. **MCP server reads** `.keboola/branch-mapping.json` for branch resolution
2. **MCP server can use** `kbc branch` CLI commands for branch management
3. **Branch context** is automatically set for all MCP operations

The generated `CLAUDE.md` file provides instructions for AI agents to:
- Use `kbc branch link` before starting work on new branches
- Use `kbc sync` commands for synchronization
- Understand the git-to-Keboola branch mapping

---

## 10. Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | General error (git not found, branch not linked, etc.) |
| `2` | Keboola authentication error |

---

## 11. Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `KBC_STORAGE_API_TOKEN` | Yes | Keboola Storage API token |
| `KBC_STORAGE_API_HOST` | Yes | Keboola stack host |
| `KBC_BRANCH_ID` | Auto-set | Set automatically by git-branching mode |

---

## 12. Appendix

### A. Migration from MCP-Only Setup

If you have an existing MCP server setup with `branch-mapping.json` at project root:

```bash
# Move existing mappings
mv branch-mapping.json .keboola/branch-mapping.json

# Re-initialize to update manifest
kbc init --git-branching --force

# Commit the changes
git add .keboola/ CLAUDE.md AGENTS.md .mcp.json
git commit -m "Migrate to CLI git-branching mode"
```

### B. Comparison with MCP Server Approach

| Aspect | MCP Server (v1) | CLI Native (v2) |
|--------|-----------------|-----------------|
| Mapping storage | Project root, gitignored | `.keboola/`, committed |
| Branch linking | MCP tool only | CLI command |
| Enforcement | MCP server level | CLI level |
| Team sharing | Per-developer | Shared via git |
| AI agent support | Required | Optional (enhanced) |

### C. Related Documentation

- Keboola CLI Structure: https://developers.keboola.com/cli/structure/
- MCP Protocol: https://modelcontextprotocol.io/
