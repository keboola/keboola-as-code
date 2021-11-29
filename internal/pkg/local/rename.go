package local

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *Manager) rename(actions []model.RenameAction) error {
	// Nothing to do
	if len(actions) == 0 {
		return nil
	}

	// Execute
	errors := utils.NewMultiError()
	warnings := utils.NewMultiError()
	var newPaths []string
	var pathsToRemove []string
	m.logger.Debugf(`Starting renaming of the %d paths.`, len(actions))
	for _, action := range actions {
		// Deep copy
		err := m.fs.Copy(action.RenameFrom, action.NewPath)

		if err != nil {
			errors.AppendWithPrefix(fmt.Sprintf(`cannot copy "%s"`, action.Description), err)
		} else {
			// Update manifest
			if err := m.manifest.PersistRecord(action.Record); err != nil {
				errors.AppendWithPrefix(fmt.Sprintf(`cannot persist "%s"`, action.Record.Desc()), err)
			}
			if filesystem.IsFrom(action.NewPath, action.Record.Path()) {
				action.Record.RenameRelatedPaths(action.RenameFrom, action.NewPath)
			}

			// Remove old path
			newPaths = append(newPaths, action.NewPath)
			pathsToRemove = append(pathsToRemove, action.RenameFrom)
		}
	}

	if errors.Len() == 0 {
		// No error -> remove old paths
		m.logger.Debug("Removing old paths.")
		for _, oldPath := range pathsToRemove {
			if err := m.fs.Remove(oldPath); err != nil {
				warnings.AppendWithPrefix(fmt.Sprintf(`cannot remove \"%s\"`, oldPath), err)
			}
		}
	} else {
		// An error occurred -> keep old state -> remove new paths
		m.logger.Debug("An error occurred, reverting rename.")
		for _, newPath := range newPaths {
			if err := m.fs.Remove(newPath); err != nil {
				warnings.AppendWithPrefix(fmt.Sprintf(`cannot remove \"%s\"`, newPath), err)
			}
		}
		m.logger.Info(`Error occurred, the rename operation was reverted.`)
	}

	// Log warnings
	if warnings.Len() > 0 {
		m.logger.Warn(utils.PrefixError(`Warning: cannot finish objects renaming`, warnings))
	}

	return errors.ErrorOrNil()
}
