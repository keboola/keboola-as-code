package export

import (
	"context"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type validateDependencies interface {
	Dialogs() *dialog.Dialogs
	Fs() filesystem.Fs
}

// isAllowedFile checks if a file is allowed to exist in the directory without prompting.
func isAllowedFile(name string) bool {
	switch name {
	case ".keboola", ".gitignore", ".git":
		return true
	}
	return strings.HasPrefix(name, ".env")
}

// validateDirectory checks if the current directory is suitable for export.
// It allows .keboola/, .env*, .gitignore, and .git to exist.
// If other files exist, it prompts for confirmation unless --force is set.
func validateDirectory(ctx context.Context, d validateDependencies, f Flags) error {
	fs := d.Fs()

	// List files in the current directory
	entries, err := fs.ReadDir(ctx, ".")
	if err != nil {
		return errors.Errorf("cannot read directory: %w", err)
	}

	// Check for files that are not in the allowed list
	conflicts := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !isAllowedFile(name) {
			conflicts = append(conflicts, name)
		}
	}

	// If no conflicts, proceed
	if len(conflicts) == 0 {
		return nil
	}

	// If --force is set, proceed with warning
	if f.Force.Value {
		return nil
	}

	// Prompt for confirmation, including the list of conflicting files
	label := "Directory contains existing files: " + strings.Join(conflicts, ", ") + ". Do you want to continue?"
	confirmed := d.Dialogs().Confirm(&prompt.Confirm{
		Label:   label,
		Default: false,
	})

	if !confirmed {
		return errors.New("export cancelled by user")
	}

	return nil
}
