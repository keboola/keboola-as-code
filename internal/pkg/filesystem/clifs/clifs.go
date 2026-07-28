// nolint:forbidigo
// Package clifs provides detection of the working directory for the CLI.
package clifs

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// New searches for a local directory known for the CLI.
// Result is a directory local filesystem or an error.
//
// It searches in the working directory, and it's each parent for:
//  1. ".keboola" directory, this indicates a local project or templates repository.
//  2. "dbt_project.yml" file, this indicates a dbt project.
//  3. If nothing can be found, returns the current working directory.
func New(ctx context.Context, opts ...filesystem.Option) (fs filesystem.Fs, err error) {
	config := filesystem.ProcessOptions(opts)

	if config.WorkingDir == "" {
		config.WorkingDir, err = os.Getwd()
		if err != nil {
			return nil, errors.Errorf(`cannot get working dir from OS: %w`, err)
		}
	}

	// Convert working directory path to absolute
	config.WorkingDir, err = filepath.Abs(config.WorkingDir)
	if err != nil {
		return nil, err
	}

	// Find root directory
	rootDir, err := find(ctx, config.Logger, config.WorkingDir)
	if err != nil {
		return nil, err
	}

	// Get relative path to the working directory
	workingDirRel, err := filepath.Rel(rootDir, config.WorkingDir)
	if err != nil {
		return nil, errors.Errorf(`cannot determine working dir relative path: %w`, err)
	}

	// Create filesystem abstraction
	opts = append(opts, filesystem.WithWorkingDir(workingDirRel))
	return aferofs.NewLocalFs(rootDir, opts...)
}

// Find searches for a directory known for the CLI. See New function.
func find(ctx context.Context, logger log.Logger, workingDir string) (string, error) {
	// Working dir must be absolute

	if !filepath.IsAbs(workingDir) {
		return "", errors.Errorf(`working directory "%s" must be absolute`, workingDir)
	}

	// Check if working dir exists
	s, err := os.Stat(workingDir)
	switch {
	case err != nil && os.IsNotExist(err):
		return "", errors.Errorf(`working directory "%s" not found`, workingDir)
	case err != nil:
		return "", errors.Errorf(`working directory "%s" is invalid: %w`, workingDir, err)
	case !s.IsDir():
		return "", errors.Errorf(`working directory "%s" is not directory`, workingDir)
	}

	sep := string(os.PathSeparator)
	actualDir := workingDir

	for {
		// Check ".keboola" dir, it must contain a project or a repository manifest,
		// otherwise it is an unrelated directory that only happens to be named ".keboola"
		// (for example a config dir created by some other tool) and must not be mistaken for a project root.
		metadataDir := filepath.Join(actualDir, filesystem.MetadataDir)
		if stat, err := os.Stat(metadataDir); err == nil {
			if stat.IsDir() {
				projectManifestPath := filepath.Join(actualDir, filesystem.FromSlash(projectManifest.Path()))
				repositoryManifestPath := filepath.Join(actualDir, filesystem.FromSlash(repositoryManifest.Path()))
				if isFile(projectManifestPath) || isFile(repositoryManifestPath) {
					return actualDir, nil
				}
				logger.Debugf(ctx, "Found \"%s\" dir, but it contains no manifest, ignoring", metadataDir)
			} else {
				logger.Debugf(ctx, "Expected dir, but found file at \"%s\"", metadataDir)
			}
		} else if !os.IsNotExist(err) {
			logger.Debugf(ctx, "Cannot check if path \"%s\" exists: %s", metadataDir, err)
		}

		// Check "dbt_project.yml"
		dbtFile := filepath.Join(actualDir, dbt.ProjectFilePath)
		if stat, err := os.Stat(dbtFile); err == nil {
			if !stat.IsDir() {
				return actualDir, nil
			} else {
				logger.Debugf(ctx, "Expected file, but found dir at \"%s\"", dbtFile)
			}
		} else if !os.IsNotExist(err) {
			logger.Debugf(ctx, "Cannot check if path \"%s\" exists: %s", dbtFile, err)
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

// isFile - true if path exists, and it is a regular file.
func isFile(path string) bool {
	stat, err := os.Stat(path)
	return err == nil && !stat.IsDir()
}
