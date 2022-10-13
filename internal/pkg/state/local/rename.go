package local

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *Manager) rename(actions []model.RenameAction) error {
	// Nothing to do
	if len(actions) == 0 {
		return nil
	}

	// Evaluate
	errs := errors.NewMultiError()
	warnings := errors.NewMultiError()
	var newPaths []string
	var pathsToRemove []string
	m.logger.Debugf(`Starting renaming of the %d paths.`, len(actions))
	for _, action := range actions {
		// Deep copy
		err := m.fs.Copy(action.RenameFrom, action.NewPath)

		if err != nil {
			errs.AppendWithPrefixf(err, `cannot copy "%s"`, action.Description)
		} else {
			// Update manifest
			if err := m.manifest.PersistRecord(action.Manifest); err != nil {
				errs.AppendWithPrefixf(err, `cannot persist "%s"`, action.Manifest.Desc())
			}
			if filesystem.IsFrom(action.NewPath, action.Manifest.Path()) {
				action.Manifest.RenameRelatedPaths(action.RenameFrom, action.NewPath)
			}

			// Remove old path
			newPaths = append(newPaths, action.NewPath)
			pathsToRemove = append(pathsToRemove, action.RenameFrom)
		}
	}

	if errs.Len() == 0 {
		// No error -> remove old paths
		m.logger.Debug("Removing old paths.")
		for _, oldPath := range pathsToRemove {
			if err := m.fs.Remove(oldPath); err != nil {
				warnings.AppendWithPrefixf(err, `cannot remove \"%s\"`, oldPath)
			}
		}
	} else {
		// An error occurred -> keep old state -> remove new paths
		m.logger.Debug("An error occurred, reverting rename.")
		for _, newPath := range newPaths {
			if err := m.fs.Remove(newPath); err != nil {
				warnings.AppendWithPrefixf(err, `cannot remove \"%s\"`, newPath)
			}
		}
		m.logger.Info(`Error occurred, the rename operation was reverted.`)
	}

	// Log warnings
	if warnings.Len() > 0 {
		m.logger.Warn(errors.PrefixError(warnings, `Warning: cannot finish objects renaming`))
	}

	return errs.ErrorOrNil()
}
