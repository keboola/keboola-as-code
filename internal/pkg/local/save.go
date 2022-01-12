package local

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type modelWriter struct {
	*Manager
	*model.LocalSaveRecipe
	backups map[string]string
	errors  *utils.MultiError
}

// saveObject to manifest and filesystem.
func (m *Manager) saveObject(manifest model.ObjectManifest, object model.Object, changedFields model.ChangedFields) error {
	if manifest.Key() != object.Key() {
		panic(fmt.Errorf(`manifest "%T" and object "%T" type mismatch`, manifest, object))
	}

	objectClone := deepcopy.Copy(object).(model.Object)
	w := modelWriter{
		Manager:         m,
		LocalSaveRecipe: &model.LocalSaveRecipe{ChangedFields: changedFields, Object: objectClone, ObjectManifest: manifest},
		backups:         make(map[string]string),
		errors:          utils.NewMultiError(),
	}
	return w.save()
}

func (w *modelWriter) save() error {
	// Validate
	if err := validator.Validate(w.Object); err != nil {
		w.errors.AppendWithPrefix(fmt.Sprintf(`%s "%s" is invalid`, w.Kind().Name, w.Path()), err)
		return w.errors
	}

	// Add record to manifest content + mark it for saving
	if err := w.manifest.PersistRecord(w.ObjectManifest); err != nil {
		return err
	}

	// Call mappers
	if err := w.mapper.MapBeforeLocalSave(w.LocalSaveRecipe); err != nil {
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
	w.ObjectManifest.ClearRelatedPaths()
	for _, path := range toDelete {
		if err := w.softDelete(path); err != nil {
			w.errors.Append(err)
			return
		}
	}

	// Write new files
	for _, fileRaw := range w.Files.All() {
		// Convert to File, eg. JsonFile -> File
		file, err := fileRaw.ToFile()
		if err != nil {
			w.errors.Append(err)
			continue
		}

		// Write
		w.ObjectManifest.AddRelatedPath(file.GetPath())
		if err := w.fs.WriteFile(file); err != nil {
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
	if !w.fs.IsFile(src) {
		return nil
	}

	err := w.fs.Move(src, dst)
	if err == nil {
		w.backups[src] = dst
	}
	return err
}

// restoreBackups if operation fails.
func (w *modelWriter) restoreBackups() {
	if w.errors.Len() > 0 {
		for dst, src := range w.backups {
			if err := w.fs.Move(src, dst); err != nil {
				w.logger.Debug(fmt.Errorf(`cannot restore backup "%s" -> "%s": %w`, src, dst, err))
			}
		}
	}
}

// removeBackups if all is ok.
func (w *modelWriter) removeBackups() {
	for _, path := range w.backups {
		if err := w.fs.Remove(path); err != nil {
			w.logger.Debug(fmt.Errorf(`cannot remove backup "%s": %w`, path, err))
		}
	}
	w.backups = make(map[string]string)
}
