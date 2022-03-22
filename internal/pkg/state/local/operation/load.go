package operation

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// LoadObject from manifest and filesystem.
func (m *Manager) LoadObject(ctx context.Context, manifest model.ObjectManifest, object model.Object) (found bool, err error) {
	// Check if directory exists
	if !m.fs.IsDir(manifest.String()) {
		return false, fmt.Errorf(`%s "%s" not found`, manifest.Kind().Name, manifest.String())
	}

	// Call mappers
	errors := utils.NewMultiError()
	recipe := model.NewLocalLoadRecipe(m.fileLoader, manifest, object)
	if err := m.mapper.MapAfterLocalLoad(recipe); err != nil {
		errors.Append(err)
	}

	// Set related paths
	for _, file := range recipe.Files.Loaded() {
		manifest.AddRelatedPath(file.Path())
	}

	// Validate, if all files loaded without error
	if errors.Len() == 0 {
		if err := validator.Validate(ctx, object); err != nil {
			errors.AppendWithPrefix(fmt.Sprintf(`%s "%s" is invalid`, manifest.Kind().Name, manifest.String()), err)
		}
	}

	return true, errors.ErrorOrNil()
}
