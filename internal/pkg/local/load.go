package local

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// loadObject from manifest and filesystem.
func (m *Manager) loadObject(manifest model.ObjectManifest, object model.Object) (found bool, err error) {
	// Check if directory exists
	if !m.fs.IsDir(manifest.Path()) {
		return false, fmt.Errorf(`%s "%s" not found`, manifest.Kind().Name, manifest.Path())
	}

	// Call mappers
	errors := utils.NewMultiError()
	recipe := model.NewLocalLoadRecipe(manifest, object)
	if err := m.mapper.MapAfterLocalLoad(recipe); err != nil {
		errors.Append(err)
	}

	// Set related paths
	for _, file := range recipe.Files.All() {
		manifest.AddRelatedPath(file.Path())
	}

	// Validate, if all files loaded without error
	if errors.Len() == 0 {
		if err := validator.Validate(object); err != nil {
			errors.AppendWithPrefix(fmt.Sprintf(`%s "%s" is invalid`, manifest.Kind().Name, manifest.Path()), err)
		}
	}

	return true, errors.ErrorOrNil()
}
