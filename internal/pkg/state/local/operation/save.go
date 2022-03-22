package operation

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type objectWriter struct {
	*Manager
	*model.LocalSaveRecipe
	ctx     context.Context
	backups map[string]string
	errors  *utils.MultiError
}

// SaveObject to manifest and filesystem.
func (m *Manager) SaveObject(ctx context.Context, object model.Object, changedFields model.ChangedFields) error {
	path := m.pathTo(object.Key())
	objectClone := deepcopy.Copy(object).(model.Object)
	w := objectWriter{
		Manager:         m,
		LocalSaveRecipe: model.NewLocalSaveRecipe(path, objectClone, changedFields),
		ctx:             ctx,
		backups:         make(map[string]string),
		errors:          utils.NewMultiError(),
	}
	return w.save()
}

func (w *objectWriter) save() error {
	// Validate
	if err := validator.Validate(w.ctx, w.Object); err != nil {
		w.errors.AppendWithPrefix(fmt.Sprintf(`%s "%s" is invalid`, w.Object.Kind().Name, w.Path.String()), err)
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

func (w *objectWriter) write() {
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
	relatedPaths := model.RelatedPaths()
	for _, file := range w.Files.All() {
		// Convert to File, eg. JsonFile -> File
		fileRaw, err := file.ToRawFile()
		if err != nil {
			w.errors.Append(err)
			continue
		}

		// Write
		w.ObjectManifest.AddRelatedPath(fileRaw.Path())
		if err := w.fs.WriteFile(fileRaw); err != nil {
			w.errors.Append(err)
		}
	}

	// Stop on error - restore backups
	if w.errors.Len() > 0 {
		return
	}

	// Cleanup - remove backups
	w.removeBackups()

	// Update related paths
}

func (w *objectWriter) softDelete(path string) error {
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
func (w *objectWriter) restoreBackups() {
	if w.errors.Len() > 0 {
		for dst, src := range w.backups {
			if err := w.fs.Move(src, dst); err != nil {
				w.logger.Debug(fmt.Errorf(`cannot restore backup "%s" -> "%s": %w`, src, dst, err))
			}
		}
	}
}

// removeBackups if all is ok.
func (w *objectWriter) removeBackups() {
	for _, path := range w.backups {
		if err := w.fs.Remove(path); err != nil {
			w.logger.Debug(fmt.Errorf(`cannot remove backup "%s": %w`, path, err))
		}
	}
	w.backups = make(map[string]string)
}
