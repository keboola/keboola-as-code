package relations

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapAfterLocalLoad - load relations from manifest to object.
func (m *relationsMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	manifest, ok := recipe.ObjectManifest.(model.ObjectManifestWithRelations)
	if !ok {
		return nil
	}

	object, ok := recipe.Object.(model.ObjectWithRelations)
	if !ok {
		return nil
	}

	object.SetRelations(manifest.GetRelations())
	return nil
}
