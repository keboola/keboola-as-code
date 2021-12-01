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
	errors *utils.MultiError
}

// loadObject from manifest and filesystem.
func (m *Manager) loadObject(manifest model.ObjectManifest, object model.Object) (found bool, err error) {
	l := &modelLoader{
		Manager:         m,
		LocalLoadRecipe: &model.LocalLoadRecipe{Object: object, ObjectManifest: manifest},
		errors:          utils.NewMultiError(),
	}
	return l.load()
}

func (l *modelLoader) load() (found bool, err error) {
	// Check if directory exists
	if !l.fs.IsDir(l.ObjectManifest.Path()) {
		l.errors.Append(fmt.Errorf(`%s "%s" not found`, l.ObjectManifest.Kind().Name, l.ObjectManifest.Path()))
		return false, l.errors.ErrorOrNil()
	}

	// Load
	l.loadFiles()
	l.transform()

	// Validate, if all files loaded without error
	if l.errors.Len() == 0 {
		if err := validator.Validate(l.Object); err != nil {
			l.errors.AppendWithPrefix(fmt.Sprintf(`%s "%s" is invalid`, l.ObjectManifest.Kind().Name, l.ObjectManifest.Path()), err)
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
	path := l.Naming().MetaFilePath(l.ObjectManifest.Path())
	desc := l.ObjectManifest.Kind().Name + " metadata"
	if file, err := l.fs.ReadJsonFieldsTo(path, desc, l.Object, model.MetaFileTag); err != nil {
		l.errors.Append(err)
	} else if file != nil {
		l.Metadata = file
		l.ObjectManifest.AddRelatedPath(path)
	}
}

// loadConfigFile from config.json.
func (l *modelLoader) loadConfigFile() {
	// config.json
	path := l.Naming().ConfigFilePath(l.ObjectManifest.Path())
	desc := l.ObjectManifest.Kind().Name
	if file, err := l.fs.ReadJsonMapTo(path, desc, l.Object, model.ConfigFileTag); err != nil {
		l.errors.Append(err)
	} else if file != nil {
		l.Configuration = file
		l.ObjectManifest.AddRelatedPath(path)
	}
}

// loadDescriptionFile from description.md.
func (l *modelLoader) loadDescriptionFile() {
	path := l.Naming().DescriptionFilePath(l.ObjectManifest.Path())
	desc := l.ObjectManifest.Kind().Name + " description"
	if file, err := l.fs.ReadFileContentTo(path, desc, l.Object, model.DescriptionFileTag); err != nil {
		l.errors.Append(err)
	} else if file != nil {
		l.Description = file
		l.ObjectManifest.AddRelatedPath(path)
	}
}

func (l *modelLoader) transform() {
	if err := l.mapper.MapAfterLocalLoad(l.LocalLoadRecipe); err != nil {
		l.errors.Append(err)
	}
}
