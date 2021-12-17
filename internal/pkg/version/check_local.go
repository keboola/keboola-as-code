package version

import (
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

// Version field from manifest.
type versionInfo struct {
	Version int `json:"version"`
}

func CheckLocalVersion(logger *zap.SugaredLogger, fs filesystem.Fs, manifestPath string) (err error) {
	// Read version first
	info := &versionInfo{}
	if err := fs.ReadJsonFileTo(manifestPath, ``, info); err != nil {
		return err
	}

	if info.Version < 1 || info.Version > 2 {
		return fmt.Errorf(`unknown version "%d" found in "%s"`, info.Version, manifestPath)
	}

	if info.Version == 1 {
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
