// nolint:forbidigo
// Package clifs provides detection of the working directory for the CLI.
package clifs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// New searches for a local directory known for the CLI.
// Result is a directory local filesystem or an error.
//
// It searches in the working directory, and it's each parent for:
//  1. ".keboola" directory, this indicates a local project or templates repository.
//  2. "dbt_project.yml" file, this indicates a dbt project.
//  3. If nothing can be found, returns the current working directory.
func New(logger log.Logger, workingDir string) (fs filesystem.Fs, err error) {
	if workingDir == "" {
		workingDir, err = os.Getwd()
		if err != nil {
			return nil, fmt.Errorf(`cannot get working dir from OS: %w`, err)
		}
	}

	// Convert working directory path to absolute
	workingDir, err = filepath.Abs(workingDir)
	if err != nil {
		return nil, err
	}

	// Find root directory
	rootDir, err := find(logger, workingDir)
	if err != nil {
		return nil, err
	}

	// Get relative path to the working directory
	workingDirRel, err := filepath.Rel(rootDir, workingDir)
	if err != nil {
		return nil, fmt.Errorf(`cannot determine working dir relative path: %w`, err)
	}

	// Create filesystem abstraction
	return aferofs.NewLocalFs(logger, rootDir, workingDirRel)
}

// Find searches for a directory known for the CLI. See New function.
func find(logger log.Logger, workingDir string) (string, error) {
	// Working dir must be absolute

	if !filepath.IsAbs(workingDir) {
		return "", fmt.Errorf(`working directory "%s" must be absolute`, workingDir)
	}

	// Check if working dir exists
	s, err := os.Stat(workingDir)
	switch {
	case err != nil && os.IsNotExist(err):
		return "", fmt.Errorf(`working directory "%s" not found`, workingDir)
	case err != nil:
		return "", fmt.Errorf(`working directory "%s" is invalid: %w`, workingDir, err)
	case !s.IsDir():
		return "", fmt.Errorf(`working directory "%s" is not directory`, workingDir)
	}

	sep := string(os.PathSeparator)
	actualDir := workingDir

	for {
		// Check ".keboola" dir
		metadataDir := filepath.Join(actualDir, filesystem.MetadataDir)
		if stat, err := os.Stat(metadataDir); err == nil {
			if stat.IsDir() {
				return actualDir, nil
			} else {
				logger.Debugf(fmt.Sprintf("Expected dir, but found file at \"%s\"", metadataDir))
			}
		} else if !os.IsNotExist(err) {
			logger.Debugf(fmt.Sprintf("Cannot check if path \"%s\" exists: %s", metadataDir, err))
		}

		// Check "dbt_project.yml"
		dbtFile := filepath.Join(actualDir, dbt.ProjectFile)
		if stat, err := os.Stat(dbtFile); err == nil {
			if !stat.IsDir() {
				return actualDir, nil
			} else {
				logger.Debugf(fmt.Sprintf("Expected file, but found dir at \"%s\"", dbtFile))
			}
		} else if !os.IsNotExist(err) {
			logger.Debugf(fmt.Sprintf("Cannot check if path \"%s\" exists: %s", dbtFile, err))
		}

		// Go up to the parent directory
		actualDir = filepath.Dir(actualDir)

		// Is root dir? -> ends with separator, or has no separator -> break
		if strings.HasSuffix(actualDir, sep) || strings.Count(actualDir, sep) == 0 {
			break
		}
	}

	return workingDir, nil
}
