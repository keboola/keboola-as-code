package local

import (
	"context"

	"github.com/keboola/go-utils/pkg/deepcopy"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type modelWriter struct {
	*Manager
	*model.LocalSaveRecipe
	ctx     context.Context
	backups map[string]string
	errors  errors.MultiError
}

// saveObject to manifest and filesystem.
func (m *Manager) saveObject(ctx context.Context, manifest model.ObjectManifest, object model.Object, changedFields model.ChangedFields) error {
	if manifest.Key() != object.Key() {
		panic(errors.Errorf(`manifest "%T" and object "%T" type mismatch`, manifest, object))
	}

	objectClone := deepcopy.Copy(object).(model.Object)
	w := modelWriter{
		Manager:         m,
		LocalSaveRecipe: model.NewLocalSaveRecipe(manifest, objectClone, changedFields),
		ctx:             ctx,
		backups:         make(map[string]string),
		errors:          errors.NewMultiError(),
	}
	return w.save()
}

func (w *modelWriter) save() error {
	// Validate
	if err := w.validator.Validate(w.ctx, w.Object); err != nil {
		w.errors.AppendWithPrefixf(err, `%s "%s" is invalid`, w.Kind().Name, w.Path())
		return w.errors
	}

	// Add record to manifest content + mark it for saving
	if err := w.manifest.PersistRecord(w.ObjectManifest); err != nil {
		return err
	}

	// Call mappers
	if err := w.mapper.MapBeforeLocalSave(w.ctx, w.LocalSaveRecipe); err != nil {
		w.errors.Append(err)
	}

	// Save
	if w.errors.Len() == 0 {
		w.write()
	}

	return w.errors.ErrorOrNil()
}

func (w *modelWriter) write() {
	// Existing files are backed up, if the operation fails, they will be restored
	defer w.restoreBackups()

	// Load files
	toDelete := w.ToDelete
	for _, file := range w.Files.All() {
		// Previous versions must be deleted
		toDelete = append(toDelete, file.Path())
	}

	// Delete
	w.ClearRelatedPaths()
	for _, path := range toDelete {
		if err := w.softDelete(path); err != nil {
			w.errors.Append(err)
			return
		}
	}

	// Write new files
	for _, file := range w.Files.All() {
		// Convert to File, eg. JsonFile -> File
		fileRaw, err := file.ToRawFile()
		if err != nil {
			w.errors.Append(err)
			continue
		}

		// Write
		if fileRaw.HasTag(model.FileKindProjectDescription) {
			w.AddRelatedPathInRoot(fileRaw.Path())
		} else {
			w.AddRelatedPath(fileRaw.Path())
		}
		if err := w.fs.WriteFile(w.ctx, fileRaw); err != nil {
			w.errors.Append(err)
		}
	}

	// Stop on error - restore backups
	if w.errors.Len() > 0 {
		return
	}

	// Cleanup - remove backups
	w.removeBackups()
}

func (w *modelWriter) softDelete(path string) error {
	src := path
	dst := src + `.old`
	if !w.fs.IsFile(w.ctx, src) {
		return nil
	}

	err := w.fs.Move(w.ctx, src, dst)
	if err == nil {
		w.backups[src] = dst
	}
	return err
}

// restoreBackups if operation fails.
func (w *modelWriter) restoreBackups() {
	if w.errors.Len() > 0 {
		for dst, src := range w.backups {
			if err := w.fs.Move(w.ctx, src, dst); err != nil {
				w.logger.Debug(w.ctx, errors.Errorf(`cannot restore backup "%s" -> "%s": %s`, src, dst, err).Error())
			}
		}
	}
}

// removeBackups if all is ok.
func (w *modelWriter) removeBackups() {
	for _, path := range w.backups {
		if err := w.fs.Remove(w.ctx, path); err != nil {
			w.logger.Debug(w.ctx, errors.Errorf(`cannot remove backup "%s": %s`, path, err).Error())
		}
	}
	w.backups = make(map[string]string)
}
