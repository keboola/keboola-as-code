package plan

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type renameExecutor struct {
	*RenamePlan
	logger        *zap.SugaredLogger
	fs            filesystem.Fs
	manifest      *manifest.Manifest
	state         *model.State
	errors        *utils.Error
	warnings      *utils.Error
	newPaths      []string
	pathsToRemove []string
}

func newRenameExecutor(logger *zap.SugaredLogger, manifest *manifest.Manifest, state *model.State, plan *RenamePlan) *renameExecutor {
	return &renameExecutor{
		RenamePlan: plan,
		logger:     logger,
		fs:         manifest.Fs(),
		manifest:   manifest,
		state:      state,
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
		// Deep copy
		err := e.fs.Copy(action.OldPath, action.NewPath)

		if err != nil {
			e.errors.AppendWithPrefix(fmt.Sprintf(`cannot copy "%s"`, action.Description), err)
		} else {
			// Update manifest
			if err := e.manifest.PersistRecord(action.Record); err != nil {
				e.errors.AppendWithPrefix(fmt.Sprintf(`cannot persist "%s"`, action.Record.Desc()), err)
			}
			if filesystem.IsFrom(action.NewPath, action.Record.Path()) {
				action.Record.RenameRelatedPaths(action.OldPath, action.NewPath)
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
			if err := e.fs.Remove(oldPath); err != nil {
				e.warnings.AppendWithPrefix(fmt.Sprintf(`cannot remove \"%s\"`, oldPath), err)
			}
		}
	} else {
		// An error occurred -> keep old state -> remove new paths
		e.logger.Debug("An error occurred, reverting rename.")
		for _, newPath := range e.newPaths {
			if err := e.fs.Remove(newPath); err != nil {
				e.warnings.AppendWithPrefix(fmt.Sprintf(`cannot remove \"%s\"`, newPath), err)
			}
		}
		e.logger.Info(`Error occurred, the rename operation was reverted.`)
	}

	// Reload paths state
	if e.errors.Len() == 0 {
		if err := e.state.ReloadPathsState(); err != nil {
			e.errors.Append(err)
		}
	}

	// Delete empty directories, eg. no extractor of a type left -> dir is empty
	if e.errors.Len() == 0 {
		if err := local.DeleteEmptyDirectories(e.fs, e.state.TrackedPaths()); err != nil {
			e.errors.Append(err)
		}
	}

	return e.warnings.ErrorOrNil(), e.errors.ErrorOrNil()
}
