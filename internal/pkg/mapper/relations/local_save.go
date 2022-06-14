package relations

import (
	"github.com/keboola/go-utils/pkg/deepcopy"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeLocalSave - store config relations from object to manifest.
func (m *relationsMapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	manifest, ok := recipe.ObjectManifest.(model.ObjectManifestWithRelations)
	if !ok {
		return nil
	}

	object, ok := recipe.Object.(model.ObjectWithRelations)
	if !ok {
		return nil
	}

	manifest.SetRelations(deepcopy.Copy(object.GetRelations().OnlyStoredInManifest()).(model.Relations))
	recipe.ChangedFields.Remove(`relations`)
	return nil
}
