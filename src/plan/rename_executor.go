package plan

import (
	"fmt"
	"os"

	"github.com/otiai10/copy"
	"go.uber.org/zap"

	"keboola-as-code/src/manifest"
	"keboola-as-code/src/utils"
)

type renameExecutor struct {
	*RenamePlan
	logger        *zap.SugaredLogger
	projectDir    string
	manifest      *manifest.Manifest
	errors        *utils.Error
	warnings      *utils.Error
	newPaths      []string
	pathsToRemove []string
}

func newRenameExecutor(logger *zap.SugaredLogger, projectDir string, manifest *manifest.Manifest, plan *RenamePlan) *renameExecutor {
	return &renameExecutor{
		RenamePlan: plan,
		logger:     logger,
		projectDir: projectDir,
		manifest:   manifest,
		errors:     utils.NewMultiError(),
		warnings:   utils.NewMultiError(),
	}
}

func (e *renameExecutor) invoke() (warns error, errs error) {
	// Nothing to do
	if len(e.actions) == 0 {
		return nil, nil
	}

	// Execute
	e.logger.Debugf(`Starting renaming of the %d paths.`, len(e.actions))
	for _, action := range e.actions {
		// Validate
		if err := action.Validate(); err != nil {
			panic(err)
		}

		// Deep copy
		err := copy.Copy(action.OldPath, action.NewPath, copy.Options{
			OnDirExists:   func(src, dest string) copy.DirExistsAction { return copy.Replace },
			Sync:          true,
			PreserveTimes: true,
		})

		if err != nil {
			e.errors.AppendWithPrefix(fmt.Sprintf(`cannot copy "%s"`, action.Description), err)
		} else {
			// Log info
			e.logger.Debug("Copied ", action.Description)

			// Update manifest
			if action.Record != nil {
				e.manifest.PersistRecord(action.Record)
			}

			// Remove old path
			e.newPaths = append(e.newPaths, action.NewPath)
			e.pathsToRemove = append(e.pathsToRemove, action.OldPath)
		}
	}

	if e.errors.Len() == 0 {
		// No error -> remove old paths
		e.logger.Debug("Removing old paths.")
		for _, oldPath := range e.pathsToRemove {
			if err := os.RemoveAll(oldPath); err == nil {
				e.logger.Debug("Removed ", utils.RelPath(e.projectDir, oldPath))
			} else {
				e.warnings.AppendWithPrefix(fmt.Sprintf(`cannot remove \"%s\"`, oldPath), err)
			}
		}
	} else {
		// An error occurred -> keep old state -> remove new paths
		e.logger.Debug("An error occurred, reverting rename.")
		for _, newPath := range e.newPaths {
			if err := os.RemoveAll(newPath); err == nil {
				e.logger.Debug("Removed ", utils.RelPath(e.projectDir, newPath))
			} else {
				e.warnings.AppendWithPrefix(fmt.Sprintf(`cannot remove \"%s\"`, newPath), err)
			}
		}
		e.logger.Info(`Error occurred, the rename operation was reverted.`)
	}

	return e.warnings.ErrorOrNil(), e.errors.ErrorOrNil()
}
