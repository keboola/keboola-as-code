package relations

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeLocalSave - store config relations from object to manifest.
func (m *relationsMapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	manifest, ok := recipe.Record.(model.ObjectManifestWithRelations)
	if !ok {
		return nil
	}

	object, ok := recipe.Object.(model.ObjectWithRelations)
	if !ok {
		return nil
	}

	manifest.SetRelations(object.GetRelations().OnlyStoredInManifest())
	recipe.ChangedFields.Remove(`relations`)
	return nil
}

// MapBeforeRemoteSave - modify changed fields.
func (m *relationsMapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	if recipe.ChangedFields.Has(`relations`) {
		// Relations are stored on the API side in config/row configuration
		recipe.ChangedFields.Add(`configuration`)
		recipe.ChangedFields.Remove(`relations`)
	}
	return nil
}
