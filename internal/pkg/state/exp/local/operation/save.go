package operation

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local/relatedpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
)

type objectWriter struct {
	*model.LocalSaveRecipe
	*Manager
	backups map[string]string
	errors  *utils.MultiError
}

// SaveObject to manifest and filesystem.
func (m *Manager) SaveObject(object model.Object, recipe *model.LocalSaveRecipe, changedFields model.ChangedFields) error {
	path, err := m.namingGenerator.GetOrGenerate(object)
	if err != nil {
		return err
	}

	objectClone := deepcopy.Copy(object).(model.Object)
	w := objectWriter{
		LocalSaveRecipe: model.NewLocalSaveRecipe(path, objectClone, changedFields),
		Manager:         m,
		backups:         make(map[string]string),
		errors:          utils.NewMultiError(),
	}
	return w.write()
}

func (w *objectWriter) write() error {

	// Write to filesystem
	if w.errors.Len() == 0 {
		w.writeToFs()
	}

	// Add to manifest
	if w.errors.Len() == 0 {
		w.addToManifest()
	}

	return w.errors.ErrorOrNil()
}

func (w *objectWriter) writeToFs() {
	// Existing files are backed up, if the operation fails, they will be restored
	defer w.restoreBackups()

	// Load files
	toDelete := w.ToDelete
	for _, file := range w.Files.All() {
		// Previous versions must be deleted
		toDelete = append(toDelete, file.Path())
	}

	// Delete
	for _, path := range toDelete {
		if err := w.softDelete(path); err != nil {
			w.errors.Append(err)
			return
		}
	}

	// Write new files
	relatedPaths := relatedpaths.New(w.Path)
	for _, file := range w.Files.All() {
		// Convert to File, eg. JsonFile -> File
		fileRaw, err := file.ToRawFile()
		if err != nil {
			w.errors.Append(err)
			continue
		}

		// Write
		relatedPaths.Add(fileRaw.Path())
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
	w.setRelatedPaths(w.Object, relatedPaths)
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

func (w *objectWriter) addToManifest() {
	// Create manifest
	objectManifest := w.Object.(model.ObjectManifestFactory).NewObjectManifest()

	// Set path
	objectManifest.SetPath(w.Path)

	// Set relations if they are supported
	o, ok1 := w.Object.(model.ObjectWithRelations)
	m, ok2 := objectManifest.(model.ObjectManifestWithRelations)
	if ok1 && ok2 {
		m.SetRelations(o.GetRelations().OnlyStoredInManifest())
	}

	// Add record to manifest
	if err := w.manifest.Add(objectManifest); err != nil {
		w.errors.Append(err)
	}
}
