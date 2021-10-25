package local

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type modelWriter struct {
	*Manager
	*model.LocalSaveRecipe
	backups map[string]string
	errors  *utils.Error
}

// saveObject to manifest and filesystem.
func (m *Manager) saveObject(record model.Record, object model.Object) error {
	if record.Key() != object.Key() {
		panic(fmt.Errorf(`record "%T" and object "%T" type mismatch`, record, object))
	}

	w := modelWriter{
		Manager:         m,
		LocalSaveRecipe: &model.LocalSaveRecipe{Object: object, Record: record},
		backups:         make(map[string]string),
		errors:          utils.NewMultiError(),
	}
	return w.save()
}

func (w *modelWriter) save() error {
	// Validate
	if err := validator.Validate(w.Object); err != nil {
		w.errors.AppendWithPrefix(fmt.Sprintf(`%s "%s" is invalid`, w.Record.Kind().Name, w.Record.Path()), err)
		return w.errors
	}

	// Add record to manifest content + mark it for saving
	if err := w.manifest.PersistRecord(w.Record); err != nil {
		return err
	}

	// Save
	w.createFiles()
	w.transform()
	if w.errors.Len() == 0 {
		w.write()
	}
	return w.errors.ErrorOrNil()
}

func (w *modelWriter) createFiles() {
	// meta.json
	if metadata := utils.MapFromTaggedFields(model.MetaFileTag, w.Object); metadata != nil {
		w.Metadata = filesystem.CreateJsonFile(w.Naming().MetaFilePath(w.Record.Path()), metadata)
	}

	// config.json
	if configuration := utils.MapFromOneTaggedField(model.ConfigFileTag, w.Object); configuration != nil {
		w.Configuration = filesystem.CreateJsonFile(w.Naming().ConfigFilePath(w.Record.Path()), configuration)
	}

	// description.md
	if description, found := utils.StringFromOneTaggedField(model.DescriptionFileTag, w.Object); found {
		w.Description = filesystem.CreateFile(w.Naming().DescriptionFilePath(w.Record.Path()), strings.TrimRight(description, " \r\n\t")+"\n")
	}
}

func (w *modelWriter) allFiles() []*filesystem.File {
	// Get all files
	files := make([]*filesystem.File, 0)

	// meta.json
	if jsonFile := w.Metadata; jsonFile != nil {
		if file, err := jsonFile.ToFile(); err == nil {
			files = append(files, file)
		} else {
			w.errors.Append(err)
		}
	}

	// config.json
	if jsonFile := w.Configuration; jsonFile != nil {
		if file, err := jsonFile.ToFile(); err == nil {
			files = append(files, file)
		} else {
			w.errors.Append(err)
		}
	}

	// description.md
	if file := w.Description; file != nil {
		files = append(files, file)
	}

	// other
	files = append(files, w.ExtraFiles...)
	return files
}

func (w *modelWriter) transform() {
	if err := w.mapper.BeforeLocalSave(w.LocalSaveRecipe); err != nil {
		w.errors.Append(err)
	}
}

func (w *modelWriter) write() {
	// Existing files are backed up, if the operation fails, they will be restored
	defer w.restoreBackups()

	// Load files
	toDelete := w.ToDelete
	newFiles := w.allFiles()
	for _, file := range newFiles {
		// Previous versions must be deleted
		toDelete = append(toDelete, file.Path)
	}

	// Delete
	for _, path := range toDelete {
		if err := w.softDelete(path); err != nil {
			w.errors.Append(err)
			return
		}
	}

	// Write new files
	for _, file := range newFiles {
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
