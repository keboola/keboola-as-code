package init

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/branchmapping"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/gitbranch"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Options for git-branching initialization.
type Options struct {
	DefaultBranch string // git default branch name (auto-detected if empty)
	McpPackage    string // MCP server package name
	Force         bool   // overwrite existing files
}

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	StorageAPIHost() string
}

// Run initializes git-branching mode.
func Run(ctx context.Context, fs filesystem.Fs, o Options, d dependencies) (defaultBranch string, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.gitbranching.init")
	defer span.End(&err)

	logger := d.Logger()

	// Check if git repository exists
	if !gitbranch.IsGitRepository(ctx, fs) {
		return "", errors.New("git repository not found\n\n   The --git-branching mode requires an initialized git repository.\n\n   To fix:\n   1. Initialize git: git init\n   2. (Optional) Add remote: git remote add origin <url>\n   3. Run: kbc init --git-branching")
	}

	// Determine default branch
	defaultBranch = o.DefaultBranch
	if defaultBranch == "" {
		// Try to detect from remote
		if gitbranch.HasRemote(ctx, fs) {
			if detected, err := gitbranch.DefaultBranchFromRemote(ctx, fs); err == nil {
				defaultBranch = detected
				logger.Infof(ctx, "Default branch: %s (auto-detected from remote)", defaultBranch)
			}
		}

		// Fall back to current branch if it's main/master
		if defaultBranch == "" {
			currentBranch, err := gitbranch.CurrentBranch(ctx, fs)
			if err != nil {
				return "", errors.Errorf("cannot determine current git branch: %w", err)
			}
			if gitbranch.IsDefaultBranch(currentBranch) {
				defaultBranch = currentBranch
				logger.Infof(ctx, "Default branch: %s (current branch)", defaultBranch)
			}
		}

		// Still not determined
		if defaultBranch == "" {
			currentBranch, _ := gitbranch.CurrentBranch(ctx, fs)
			return "", errors.Errorf("cannot determine default branch\n\n   Current branch: %s\n   No remote configured to detect default branch.\n\n   To fix, either:\n   1. Run from your default branch: git checkout main && kbc init --git-branching\n   2. Specify explicitly: kbc init --git-branching --default-branch=main", currentBranch)
		}
	}

	// Verify current branch is the default branch
	currentBranch, err := gitbranch.CurrentBranch(ctx, fs)
	if err != nil {
		return "", errors.Errorf("cannot determine current git branch: %w", err)
	}
	if currentBranch != defaultBranch {
		return "", errors.Errorf("must run from the default branch\n\n   Current branch: %s\n   Default branch: %s\n\n   The --git-branching init must be run from the default branch to\n   auto-link it to Keboola production.\n\n   To fix:\n   git checkout %s\n   kbc init --git-branching", currentBranch, defaultBranch, defaultBranch)
	}

	// Check for existing files (unless force)
	if !o.Force {
		existingFiles := checkExistingFiles(ctx, fs)
		if len(existingFiles) > 0 {
			msg := "agent configuration files already exist\n\n   Found existing files:\n"
			for _, f := range existingFiles {
				msg += "   - " + f + "\n"
			}
			msg += "\n   To overwrite, run: kbc init --git-branching --force"
			return "", errors.New(msg)
		}
	}

	// Create branch mapping file with default branch linked to production
	mappings := branchmapping.New()
	mappings.SetMapping(defaultBranch, &branchmapping.BranchMapping{
		ID:   nil, // null means production
		Name: "Main",
	})
	if err := branchmapping.Save(ctx, fs, mappings); err != nil {
		return "", errors.Errorf("failed to create branch mapping file: %w", err)
	}
	logger.Infof(ctx, "Created %s", branchmapping.Path())
	logger.Infof(ctx, "Linked %s → Keboola production (auto-linked)", defaultBranch)

	// Generate CLAUDE.md
	if err := generateClaudeMd(ctx, fs, logger); err != nil {
		return "", err
	}

	// Generate AGENTS.md
	if err := generateAgentsMd(ctx, fs, logger); err != nil {
		return "", err
	}

	// Generate .mcp.json
	apiHost := d.StorageAPIHost()
	if err := generateMcpJson(ctx, fs, o.McpPackage, defaultBranch, apiHost, logger); err != nil {
		return "", err
	}

	// Update .gitignore
	if err := updateGitignore(ctx, fs, logger); err != nil {
		return "", err
	}

	logger.Info(ctx, "")
	logger.Info(ctx, "Git-branching mode enabled.")

	return defaultBranch, nil
}

func checkExistingFiles(ctx context.Context, fs filesystem.Fs) []string {
	var existing []string
	files := []string{"CLAUDE.md", "AGENTS.md", ".mcp.json"}
	for _, f := range files {
		if fs.IsFile(ctx, f) {
			existing = append(existing, f)
		}
	}
	return existing
}

func generateClaudeMd(ctx context.Context, fs filesystem.Fs, logger log.Logger) error {
	content := `# Keboola CLI with Git-Branching Mode

This project is configured with Keboola CLI **Git-Branching Mode**, which maps git branches to Keboola development branches.

## Key Features

- **Automatic branch mapping** - git branches are linked to Keboola branches
- **Shared mappings** - branch mappings are committed to git, shared across team
- **CLI-native commands** - use ` + "`kbc branch`" + ` commands for branch management
- **MCP integration** - optional MCP server for enhanced AI agent support

## Branch Management Commands

| Command | Description |
|---------|-------------|
| ` + "`kbc branch link`" + ` | Links current git branch to a Keboola branch. Creates new branch if needed. **Does not work on default branch.** |
| ` + "`kbc branch unlink`" + ` | Removes the mapping for the current git branch (does not delete the Keboola branch). **Does not work on default branch.** |
| ` + "`kbc branch status`" + ` | Shows the mapping status for the current git branch. |
| ` + "`kbc branch list`" + ` | Lists all git-to-Keboola branch mappings. |

**Note:** The default branch (main/master) is automatically linked to Keboola production during ` + "`kbc init --git-branching`" + `. This mapping cannot be changed.

## Sync Commands

| Command | Description |
|---------|-------------|
| ` + "`kbc sync push`" + ` | Push local changes to Keboola |
| ` + "`kbc sync pull`" + ` | Pull remote changes from Keboola |
| ` + "`kbc sync diff`" + ` | Show differences between local and remote |

**Note:** Sync commands require the current git branch to be linked. If not linked, they will fail with guidance to run ` + "`kbc branch link`" + `.

## Workflow

### First Time Setup (on a new branch)

1. **Check current branch mapping:**
   ` + "```bash\n   kbc branch status\n   ```" + `

2. **Link branch if needed:**
   ` + "```bash\n   kbc branch link\n   # Creates Keboola branch with same name as git branch and links it\n   ```" + `

3. **Commit the mapping for teammates:**
   ` + "```bash\n   git add .keboola/branch-mapping.json\n   git commit -m \"Link git branch to Keboola branch\"\n   ```" + `

### Daily Operations

1. **Pull latest changes:**
   ` + "```bash\n   kbc sync pull\n   ```" + `

2. **Make changes to configurations** (edit files in the project)

3. **Check differences:**
   ` + "```bash\n   kbc sync diff\n   ```" + `

4. **Push changes:**
   ` + "```bash\n   kbc sync push\n   ```" + `

## Branch Context

The CLI automatically:
- Detects the current git branch
- Looks up the corresponding Keboola branch ID from ` + "`.keboola/branch-mapping.json`" + `
- Sets ` + "`KBC_BRANCH_ID`" + ` environment variable for all operations

This ensures you always work with the correct Keboola development branch.
`
	if err := fs.WriteFile(ctx, filesystem.NewRawFile("CLAUDE.md", content)); err != nil {
		return errors.Errorf("failed to create CLAUDE.md: %w", err)
	}
	logger.Info(ctx, "Created CLAUDE.md (agent instructions)")
	return nil
}

func generateAgentsMd(ctx context.Context, fs filesystem.Fs, logger log.Logger) error {
	content := `# Agent Instructions

This file provides instructions for AI coding assistants working with this Keboola CLI project.

## Git-Branching Mode

This project uses **git-branching mode** which maps git branches to Keboola development branches.

### Before Starting Work

1. Check if the current git branch is linked:
   ` + "```bash\n   kbc branch status\n   ```" + `

2. If not linked, create a mapping:
   ` + "```bash\n   kbc branch link\n   ```" + `

### Sync Operations

Always use the CLI commands for sync operations:
- ` + "`kbc sync pull`" + ` - Pull changes from Keboola
- ` + "`kbc sync push`" + ` - Push changes to Keboola
- ` + "`kbc sync diff`" + ` - View differences

### Important Notes

- The default branch (main/master) is linked to Keboola production
- Feature branches need to be explicitly linked before sync operations
- Branch mappings are stored in ` + "`.keboola/branch-mapping.json`" + ` and should be committed

See CLAUDE.md for detailed instructions.
`
	if err := fs.WriteFile(ctx, filesystem.NewRawFile("AGENTS.md", content)); err != nil {
		return errors.Errorf("failed to create AGENTS.md: %w", err)
	}
	logger.Info(ctx, "Created AGENTS.md (cross-agent instructions)")
	return nil
}

func generateMcpJson(ctx context.Context, fs filesystem.Fs, mcpPackage, defaultBranch, apiHost string, logger log.Logger) error {
	content := `{
  "mcpServers": {
    "keboola": {
      "command": "uvx",
      "args": [
        "` + mcpPackage + `",
        "--working-dir", ".",
        "--git-default-branch", "` + defaultBranch + `"
      ],
      "env": {
        "KBC_STORAGE_API_TOKEN": "${KBC_STORAGE_API_TOKEN}",
        "KBC_STORAGE_API_HOST": "${KBC_STORAGE_API_HOST}"
      }
    }
  }
}
`
	if err := fs.WriteFile(ctx, filesystem.NewRawFile(".mcp.json", content)); err != nil {
		return errors.Errorf("failed to create .mcp.json: %w", err)
	}
	logger.Info(ctx, "Created .mcp.json (MCP server configuration)")
	return nil
}

func updateGitignore(ctx context.Context, fs filesystem.Fs, logger log.Logger) error {
	gitignorePath := ".gitignore"
	var content string

	// Read existing gitignore if it exists
	if fs.IsFile(ctx, gitignorePath) {
		file, err := fs.ReadFile(ctx, filesystem.NewFileDef(gitignorePath))
		if err != nil {
			return errors.Errorf("failed to read .gitignore: %w", err)
		}
		content = file.Content
	}

	// Check if entries already exist
	entries := []string{".env", ".env.local", "*.pyc", "__pycache__/"}
	var toAdd []string
	for _, entry := range entries {
		if !containsLine(content, entry) {
			toAdd = append(toAdd, entry)
		}
	}

	if len(toAdd) == 0 {
		return nil
	}

	// Append new entries
	if content != "" && content[len(content)-1] != '\n' {
		content += "\n"
	}
	content += "\n# Keboola CLI\n"
	for _, entry := range toAdd {
		content += entry + "\n"
	}

	if err := fs.WriteFile(ctx, filesystem.NewRawFile(gitignorePath, content)); err != nil {
		return errors.Errorf("failed to update .gitignore: %w", err)
	}
	logger.Info(ctx, "Updated .gitignore")
	return nil
}

func containsLine(content, line string) bool {
	lines := splitLines(content)
	for _, l := range lines {
		if l == line {
			return true
		}
	}
	return false
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
