package relations

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
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

	object.SetRelations(deepcopy.Copy(manifest.GetRelations()).(model.Relations))
	return nil
}
