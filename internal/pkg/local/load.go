package local

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type modelLoader struct {
	*Manager
	*model.LocalLoadRecipe
	errors *utils.Error
}

// loadObject from manifest and filesystem.
func (m *Manager) loadObject(record model.Record, object model.Object) (found bool, err error) {
	l := &modelLoader{
		Manager:         m,
		LocalLoadRecipe: &model.LocalLoadRecipe{Object: object, Record: record},
		errors:          utils.NewMultiError(),
	}
	return l.load()
}

func (l *modelLoader) load() (found bool, err error) {
	// Check if directory exists
	if !l.fs.IsDir(l.Record.Path()) {
		l.errors.Append(fmt.Errorf(`%s "%s" not found`, l.Record.Kind().Name, l.Record.Path()))
		return false, l.errors.ErrorOrNil()
	}

	// Load
	l.loadFiles()
	l.transform()

	// Validate, if all files loaded without error
	if l.errors.Len() == 0 {
		if err := validator.Validate(l.Object); err != nil {
			l.errors.AppendWithPrefix(fmt.Sprintf(`%s "%s" is invalid`, l.Record.Kind().Name, l.Record.Path()), err)
		}
	}

	return true, l.errors.ErrorOrNil()
}

func (l *modelLoader) loadFiles() {
	l.loadMetaFile()
	l.loadConfigFile()
	l.loadDescriptionFile()
}

// loadMetaFile from meta.json.
func (l *modelLoader) loadMetaFile() {
	path := l.Naming().MetaFilePath(l.Record.Path())
	desc := l.Record.Kind().Name + " metadata"
	if file, err := l.fs.ReadJsonFieldsTo(path, desc, l.Object, model.MetaFileTag); err != nil {
		l.errors.Append(err)
	} else if file != nil {
		l.Metadata = file
		l.Record.AddRelatedPath(path)
	}
}

// loadConfigFile from config.json.
func (l *modelLoader) loadConfigFile() {
	// config.json
	path := l.Naming().ConfigFilePath(l.Record.Path())
	desc := l.Record.Kind().Name
	if file, err := l.fs.ReadJsonMapTo(path, desc, l.Object, model.ConfigFileTag); err != nil {
		l.errors.Append(err)
	} else if file != nil {
		l.Configuration = file
		l.Record.AddRelatedPath(path)
	}
}

// loadDescriptionFile from description.md.
func (l *modelLoader) loadDescriptionFile() {
	path := l.Naming().DescriptionFilePath(l.Record.Path())
	desc := l.Record.Kind().Name + " description"
	if file, err := l.fs.ReadFileContentTo(path, desc, l.Object, model.DescriptionFileTag); err != nil {
		l.errors.Append(err)
	} else if file != nil {
		l.Description = file
		l.Record.AddRelatedPath(path)
	}
}

func (l *modelLoader) transform() {
	if err := l.mapper.AfterLocalLoad(l.LocalLoadRecipe); err != nil {
		l.errors.Append(err)
	}
}
