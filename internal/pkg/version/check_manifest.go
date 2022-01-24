package version

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// Version field from manifest.
type versionInfo struct {
	Version *int `json:"version"`
}

func CheckManifestVersion(logger log.Logger, fs filesystem.Fs, manifestPath string) (err error) {
	// Read manifest file
	file, err := fs.ReadFile(filesystem.NewFileDef(manifestPath).SetDescription(`manifest`))
	if err != nil {
		return err
	}

	// Read version field
	info := &versionInfo{}
	if err := json.DecodeString(file.Content, info); err != nil {
		return fmt.Errorf(`cannot decode manifest "%s": %w`, manifestPath, err)
	}

	// Check version
	if info.Version == nil {
		return fmt.Errorf(`version field not found in "%s"`, manifestPath)
	}

	version := *info.Version
	if version < 1 || version > 2 {
		return fmt.Errorf(`unknown version "%d" found in "%s"`, version, manifestPath)
	}

	if version == 1 {
		warning := `
Your project needs to be migrated to the new version of the Keboola CLI.
  1. Make sure you have a backup of the current project directory (eg. git commit, git push).
  2. Then run "kbc pull --force" to overwrite local state.
  3. Manually check that there are no unexpected changes in the project directory (git diff).
		`
		logger.Warn(`Warning: `, strings.TrimLeft(warning, "\n"))
	} else {
		logger.Debugf(`Version "%d" in "%s" is up to date.`, info.Version, manifestPath)
	}

	return nil
}
