// nolint: forbidigo
package aferofs

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/localfs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/memoryfs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func NewLocalFsFindKeboolaDir(logger log.Logger, workingDir string) (fs filesystem.Fs, err error) {
	if workingDir == "" {
		workingDir, err = os.Getwd()
		if err != nil {
			return nil, fmt.Errorf(`cannot get working dir from OS: %w`, err)
		}
	}

	// Convert working dir path to absolute
	workingDir, err = filepath.Abs(workingDir)
	if err != nil {
		return nil, err
	}

	// Find project dir
	keboolaDir, err := localfs.FindKeboolaDir(logger, workingDir)
	if err != nil {
		return nil, err
	}

	workingDirRel, err := filepath.Rel(keboolaDir, workingDir)
	if err != nil {
		return nil, fmt.Errorf(`cannot determine working dir relative path: %w`, err)
	}

	backendFs, err := localfs.New(keboolaDir)
	if err != nil {
		return nil, err
	}

	// Create filesystem abstraction
	return New(logger, backendFs, workingDirRel), nil
}

func NewLocalFs(logger log.Logger, rootDir string, workingDirRel string) (fs filesystem.Fs, err error) {
	backendFs, err := localfs.New(rootDir)
	if err != nil {
		return nil, err
	}
	return New(logger, backendFs, workingDirRel), nil
}

func NewMemoryFs(logger log.Logger, workingDir string) (fs filesystem.Fs, err error) {
	return New(logger, memoryfs.New(), workingDir), nil
}
