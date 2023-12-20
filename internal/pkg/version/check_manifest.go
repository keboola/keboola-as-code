package version

import (
	"context"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Version field from manifest.
type versionInfo struct {
	Version *int `json:"version"`
}

func CheckManifestVersion(ctx context.Context, logger log.Logger, fs filesystem.Fs, manifestPath string) (err error) {
	// Read manifest file
	file, err := fs.ReadFile(ctx, filesystem.NewFileDef(manifestPath).SetDescription(`manifest`))
	if err != nil {
		return err
	}

	// Read version field
	info := &versionInfo{}
	if err := json.DecodeString(file.Content, info); err != nil {
		return errors.Errorf(`cannot decode manifest "%s": %w`, manifestPath, err)
	}

	// Check version
	if info.Version == nil {
		return errors.Errorf(`version field not found in "%s"`, manifestPath)
	}

	version := *info.Version
	if version < 1 || version > 2 {
		return errors.Errorf(`unknown version "%d" found in "%s"`, version, manifestPath)
	}

	if version == 1 {
		warning := `
Warning: Your project needs to be migrated to the new version of the Keboola CLI.
  1. Make sure you have a backup of the current project directory (eg. git commit, git push).
  2. Then run "kbc pull --force" to overwrite local state.
  3. Manually check that there are no unexpected changes in the project directory (git diff).
		`
		logger.WarnCtx(ctx, strings.TrimLeft(warning, "\n"))
	} else {
		logger.DebugfCtx(ctx, `Version "%d" in "%s" is up to date.`, version, manifestPath)
	}

	return nil
}
