package local

import (
	"context"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *Manager) rename(ctx context.Context, actions []model.RenameAction, cleanup bool) error {
	// Nothing to do
	if len(actions) == 0 {
		return nil
	}

	// Evaluate
	errs := errors.NewMultiError()
	warnings := errors.NewMultiError()
	var newPaths []string
	var pathsToRemove []string
	m.logger.Debugf(ctx, `Starting renaming of the %d paths.`, len(actions))
	for _, action := range actions {
		// Deep copy
		err := m.fs.Copy(ctx, action.RenameFrom, action.NewPath)
		if err != nil {
			// If destination exists and cleanup is enabled, attempt to remove and retry
			handled := m.handleCopyErrorWithCleanup(ctx, action, err, cleanup, &newPaths, &pathsToRemove, errs)
			if !handled {
				errs.AppendWithPrefixf(err, `cannot copy "%s"`, action.Description)
			}
			continue
		}

		// Update manifest and track paths on successful copy
		m.updateManifestAndTrackPaths(action, &newPaths, &pathsToRemove, errs)
	}

	if errs.Len() == 0 {
		// No error -> remove old paths
		// Avoid removing a path that has become a destination in this batch (chained renames)
		m.logger.Debug(ctx, "Removing old paths.")

		// Build a set of all destinations created in this batch
		newPathSet := make(map[string]struct{}, len(newPaths))
		for _, p := range newPaths {
			newPathSet[p] = struct{}{}
		}

		for _, oldPath := range pathsToRemove {
			if _, isAlsoDestination := newPathSet[oldPath]; isAlsoDestination {
				// Skip removal: this old path is also a destination of another rename in this batch
				continue
			}
			if err := m.fs.Remove(ctx, oldPath); err != nil {
				warnings.AppendWithPrefixf(err, `cannot remove \"%s\"`, oldPath)
			}
		}
	} else {
		// An error occurred -> keep old state -> remove new paths
		m.logger.Debug(ctx, "An error occurred, reverting rename.")
		for _, newPath := range newPaths {
			if err := m.fs.Remove(ctx, newPath); err != nil {
				warnings.AppendWithPrefixf(err, `cannot remove \"%s\"`, newPath)
			}
		}
		m.logger.Info(ctx, `Error occurred, the rename operation was reverted.`)
	}

	// Log warnings
	if warnings.Len() > 0 {
		err := errors.PrefixError(warnings, "cannot finish objects renaming")
		m.logger.Warn(ctx, errors.Format(errors.PrefixError(err, "warning"), errors.FormatAsSentences()))
	}

	return errs.ErrorOrNil()
}

// handleCopyErrorWithCleanup handles copy errors by attempting cleanup and retry if applicable.
// Returns true if the error was handled successfully (including retry success), false otherwise.
func (m *Manager) handleCopyErrorWithCleanup(ctx context.Context, action model.RenameAction, copyErr error, cleanup bool, newPaths, pathsToRemove *[]string, errs errors.MultiError) bool {
	if !cleanup || !strings.Contains(copyErr.Error(), "destination exists") {
		return false
	}

	// Try to remove existing destination
	if rmErr := m.fs.Remove(ctx, action.NewPath); rmErr != nil {
		errs.AppendWithPrefixf(rmErr, `cannot remove existing destination "%s"`, action.NewPath)
		return false
	}

	// Retry copy after cleanup
	if retryErr := m.fs.Copy(ctx, action.RenameFrom, action.NewPath); retryErr != nil {
		errs.AppendWithPrefixf(retryErr, `cannot copy "%s" after cleanup`, action.Description)
		return false
	}

	// Copy succeeded after cleanup - update manifest and track paths
	m.updateManifestAndTrackPaths(action, newPaths, pathsToRemove, errs)
	return true
}

// updateManifestAndTrackPaths updates the manifest and tracks paths for a successful copy operation.
func (m *Manager) updateManifestAndTrackPaths(action model.RenameAction, newPaths, pathsToRemove *[]string, errs errors.MultiError) {
	// Update manifest
	if err := m.manifest.PersistRecord(action.Manifest); err != nil {
		errs.AppendWithPrefixf(err, `cannot persist "%s"`, action.Manifest.Desc())
	}
	if filesystem.IsFrom(action.NewPath, action.Manifest.Path()) {
		action.Manifest.RenameRelatedPaths(action.RenameFrom, action.NewPath)
	}

	// Track paths
	*newPaths = append(*newPaths, action.NewPath)
	*pathsToRemove = append(*pathsToRemove, action.RenameFrom)
}
