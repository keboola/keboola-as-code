package relations

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// AfterLocalLoad - load relations from manifest to object.
func (m *relationsMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	if manifest, ok := recipe.Record.(*model.ConfigManifest); ok {
		config := recipe.Object.(*model.Config)
		config.Relations = manifest.Relations
	}
	return nil
}
