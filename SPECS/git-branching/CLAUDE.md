# Keboola CLI with Git-Branching Mode

This project is configured with Keboola CLI **Git-Branching Mode**, which maps git branches to Keboola development branches.

## Key Features

- **Automatic branch mapping** - git branches are linked to Keboola branches
- **Shared mappings** - branch mappings are committed to git, shared across team
- **CLI-native commands** - use `kbc branch` commands for branch management
- **MCP integration** - optional MCP server for enhanced AI agent support

## Branch Management Commands

| Command | Description |
|---------|-------------|
| `kbc branch link` | Links current git branch to a Keboola branch. Creates new branch if needed. **Does not work on default branch.** |
| `kbc branch unlink` | Removes the mapping for the current git branch (does not delete the Keboola branch). **Does not work on default branch.** |
| `kbc branch status` | Shows the mapping status for the current git branch. |
| `kbc branch list` | Lists all git-to-Keboola branch mappings. |

**Note:** The default branch (main/master) is automatically linked to Keboola production during `kbc init --git-branching`. This mapping cannot be changed.

## Sync Commands

| Command | Description |
|---------|-------------|
| `kbc sync push` | Push local changes to Keboola |
| `kbc sync pull` | Pull remote changes from Keboola |
| `kbc sync diff` | Show differences between local and remote |

**Note:** Sync commands require the current git branch to be linked. If not linked, they will fail with guidance to run `kbc branch link`.

## Workflow

### First Time Setup (on a new branch)

1. **Check current branch mapping:**
   ```bash
   kbc branch status
   ```

2. **Link branch if needed:**
   ```bash
   kbc branch link
   # Creates Keboola branch with same name as git branch and links it
   ```

3. **Commit the mapping for teammates:**
   ```bash
   git add .keboola/branch-mapping.json
   git commit -m "Link git branch to Keboola branch"
   ```

### Daily Operations

1. **Pull latest changes:**
   always start with pulling changes
   ```bash
   kbc sync pull
   ```

2. **Make changes to configurations** (edit files in the project)

3. **Check differences:**
   ```bash
   kbc sync diff
   ```

4. **Push changes:**
   ```bash
   kbc sync push
   ```


## Using CLI with MCP Tools

When MCP server is configured (via `.mcp.json`), you get additional capabilities:

- **CLI** represents the Keboola project in a file system - use for local editing, validation, and version control
- **MCP tools** operate directly on Keboola via API - use for running jobs, managing tables, getting component schemas

### Best Practices

- Use CLI for local editing, validation, and version control
- Use MCP tools for running jobs, managing tables, and getting component context
- The automatic branch context ensures both CLI and MCP tools operate on the correct Keboola branch
- After remote edits via MCP, always run `kbc sync pull` to keep local and remote in sync

### Working with Configurations

- When creating a new configuration, use MCP tools to search components and get schemas
- Use `kbc create config` to create local files, then edit the config files directly
- If you struggle creating a config locally, use MCP `create_config` tool, then `kbc sync pull`

**IMPORTANT:** JSON configurations are converted into `_config.yml` for readability.

### Working with Code-Based Components

For SQL, Python, R transformations and Custom Python Applications:

- First create a dummy configuration via MCP tools or `kbc create config`, then pull and edit code files directly
- SQL transformations: Edit `.sql` files within the transformation's directory
- Python/R transformations: Edit `.py` or `.r` files respectively
- Custom Python Applications: Modify code in the `application` directory

#### Testing Locally with `kbc local data`

Download input tables and create config.json and data folder structure to develop and test transformations or Custom Python Application locally:

```bash
# Config path matches the directory structure in your project
# Format: branch/component-type/component-id/config-name
kbc local data main/transformation/keboola.python-transformation-v2/my-transform

# Or use just the config name if it's unique in the project
kbc local data my-transform

# Limit rows to download (default: 1000)
kbc local data my-transform --limit 100
```

Data is saved to a `data/` directory inside the config:
```
my-transform/
├── _config.yml
├── transform.py
├── pyproject.toml
└── data/                    # Created by kbc local data
    ├── config.json          # Parameters for Common Interface
    ├── in/
    │   ├── tables/          # Downloaded input tables as CSV
    │   │   ├── users.csv
    │   │   └── users.csv.manifest
    │   └── files/           # Downloaded input files
    └── out/
        ├── tables/          # Output tables (written by your code)
        └── files/           # Output files
```

**Running locally:**

Each Python Transformation/Custom Python has a `pyproject.toml` - create a venv (ideally using `uv`) and install dependencies:

```bash
cd main/transformation/keboola.python-transformation-v2/my-transform
uv venv && uv pip install -e .
uv run python transform.py
```

## Branch Context

The CLI automatically:
- Detects the current git branch
- Looks up the corresponding Keboola branch ID from `.keboola/branch-mapping.json`
- Sets `KBC_BRANCH_ID` environment variable for all operations

This ensures you always work with the correct Keboola development branch.

## Creating Commits and PRs

1. Always run `kbc sync pull` before starting or finishing work
2. After finishing, run `kbc sync push` to push changes to Keboola
3. Create a commit with a meaningful message describing the changes
4. If you created a new branch mapping, include `.keboola/branch-mapping.json` in the commit

## Error Handling

### "Git branch not linked" Error
Run `kbc branch link` first to create a mapping.

### "Project not initialized" Error
The project must be initialized with `--allow-target-env` flag:
```bash
kbc sync init --allow-target-env --storage-api-host connection.<region>.keboola.com
```

## Project Structure

```
project-root/
├── .keboola/
│   ├── manifest.json           # Project manifest (allowTargetEnv: true, gitBranching: enabled)
│   └── branch-mapping.json     # Git-to-Keboola branch mappings (committed)
├── .mcp.json                   # MCP server configuration (optional)
├── CLAUDE.md                   # This file
├── AGENTS.md                   # Cross-agent instructions
├── main/                       # Main branch configurations
│   ├── application/            # Application configurations
│   ├── extractor/              # Extractor configurations
│   └── transformation/         # Transformation configurations
└── .gitignore
```

## Tips

- Always check `kbc branch status` before starting work to confirm branch context
- Use `kbc sync diff` before `kbc sync push` to review changes
- The `.keboola/branch-mapping.json` file is committed - teammates get mappings automatically
- Main/master git branches are auto-linked to Keboola production (cannot be changed via link/unlink)
- When switching git branches, the CLI automatically uses the correct Keboola branch
- Feature branches need `kbc branch link` before first use
