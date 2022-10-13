package local

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// loadObject from manifest and filesystem.
func (m *Manager) loadObject(ctx context.Context, manifest model.ObjectManifest, object model.Object) (found bool, err error) {
	// Check if directory exists
	if !m.fs.IsDir(manifest.Path()) {
		return false, errors.Errorf(`%s "%s" not found`, manifest.Kind().Name, manifest.Path())
	}

	// Call mappers
	errs := errors.NewMultiError()
	recipe := model.NewLocalLoadRecipe(m.FileLoader(), manifest, object)
	if err := m.mapper.MapAfterLocalLoad(context.Background(), recipe); err != nil {
		errs.Append(err)
	}

	// Set related paths
	for _, file := range recipe.Files.Loaded() {
		if file.HasTag(model.FileKindProjectDescription) {
			manifest.AddRelatedPathInRoot(file.Path())
		} else {
			manifest.AddRelatedPath(file.Path())
		}
	}

	// Validate, if all files loaded without error
	if errs.Len() == 0 {
		if err := m.validator.Validate(ctx, object); err != nil {
			errs.AppendWithPrefixf(err, `%s "%s" is invalid`, manifest.Kind().Name, manifest.Path())
		}
	}

	return true, errs.ErrorOrNil()
}
