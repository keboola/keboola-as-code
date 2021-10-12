// nolint: forbidigo
package aferofs

import (
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/localfs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/memoryfs"
)

func NewLocalFsFindProjectDir(logger *zap.SugaredLogger, workingDir string) (fs filesystem.Fs, err error) {
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
	projectDir, err := localfs.FindProjectDir(logger, workingDir)
	if err != nil {
		return nil, err
	}

	workingDirRel, err := filepath.Rel(projectDir, workingDir)
	if err != nil {
		return nil, fmt.Errorf(`cannot determine working dir relative path: %w`, err)
	}

	// Create filesystem abstraction
	return New(logger, localfs.New(projectDir), workingDirRel), nil
}

func NewLocalFs(logger *zap.SugaredLogger, projectDir string, workingDirRel string) (fs filesystem.Fs, err error) {
	// Create filesystem abstraction
	return New(logger, localfs.New(projectDir), workingDirRel), nil
}

func NewMemoryFs(logger *zap.SugaredLogger, workingDir string) (fs filesystem.Fs, err error) {
	// Create filesystem abstraction
	return New(logger, memoryfs.New(), workingDir), nil
}
