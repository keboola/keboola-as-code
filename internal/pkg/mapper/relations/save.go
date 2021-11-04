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

	oldParentKey, err := manifest.ParentKey()
	if err != nil {
		return err
	}

	manifest.SetRelations(object.GetRelations().OnlyStoredInManifest())

	newParentKey, err := manifest.ParentKey()
	if err != nil {
		return err
	}

	if oldParentKey != newParentKey {
		manifest.State().ParentChanged = true
	}

	return nil
}
