package local

import (
	"context"
	"io/fs"
	"sort"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// DeleteInvalidObjects from disk, for example if pull --force used.
func (m *Manager) DeleteInvalidObjects(ctx context.Context) error {
	errs := errors.NewMultiError()
	for _, objectManifest := range m.manifest.All() {
		if objectManifest.State().IsInvalid() {
			if err := m.deleteObject(ctx, objectManifest); err != nil {
				errs.Append(err)
			}
		}
	}
	return errs.ErrorOrNil()
}

// DeleteEmptyDirectories from project directory (eg. dir with extractors, but no extractor left)
// Deleted are only empty directories from know/tracked paths.
// Hidden dirs are ignored.
func DeleteEmptyDirectories(ctx context.Context, fs filesystem.Fs, trackedPaths []string) error {
	errs := errors.NewMultiError()
	emptyDirs := orderedmap.New()
	root := `.`
	err := fs.Walk(ctx, root, func(path string, info filesystem.FileInfo, err error) error {
		// Stop on error
		if err != nil {
			return err
		}

		// Ignore root
		if path == root {
			return nil
		}

		// Stop on ignored dir
		skipDir := isIgnoredDir(path, info)

		// Found a directory -> store path
		if !skipDir && info.IsDir() {
			emptyDirs.Set(path, true)
			return nil
		}

		// Found file/ignored dir -> all parent dirs are not empty
		emptyDirs.DeleteFunc(func(key string) bool {
			return filesystem.IsFrom(path, key)
		})

		// Skip sub-directories
		if skipDir {
			return filesystem.SkipDir
		}

		return nil
	})
	if err != nil {
		return err
	}

	// Sort by the longest path firs -> delete most nested directory first
	emptyDirs.SortKeys(func(keys []string) {
		sort.SliceStable(keys, func(i, j int) bool {
			return len(keys[i]) > len(keys[j])
		})
	})

	// Remove only empty dirs, if parent dir is from tracked dirs
	dirsToRemove := make([]string, 0)
	for _, dir := range emptyDirs.Keys() {
		for _, tracked := range trackedPaths {
			if tracked == dir || filesystem.IsFrom(dir, tracked) {
				// Remove dir, it is from a tracked dir
				dirsToRemove = append(dirsToRemove, dir)
				break
			}
		}
	}

	// Delete
	for _, dir := range dirsToRemove {
		if err := fs.Remove(ctx, dir); err != nil {
			errs.Append(err)
		}
	}

	return errs.ErrorOrNil()
}

// deleteObject from manifest and filesystem.
func (m *Manager) deleteObject(ctx context.Context, objectManifest model.ObjectManifest) error {
	errs := errors.NewMultiError()

	// Remove manifest from manifest content
	m.manifest.Delete(objectManifest)

	// Remove all related files
	for _, path := range objectManifest.GetRelatedPaths() {
		if m.fs.IsFile(ctx, path) {
			if err := m.fs.Remove(ctx, path); err != nil {
				errs.Append(err)
			}
		}
	}

	return errs.ErrorOrNil()
}

func isIgnoredDir(path string, info fs.FileInfo) bool {
	base := filesystem.Base(path)
	return info.IsDir() && strings.HasPrefix(base, ".")
}
