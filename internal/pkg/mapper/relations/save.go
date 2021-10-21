package relations

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// BeforeLocalSave - store config relations from object to manifest.
func (m *relationsMapper) BeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	if manifest, ok := recipe.Record.(*model.ConfigManifest); ok {
		config := recipe.Object.(*model.Config)
		manifest.Relations = config.Relations
	}
	return nil
}
