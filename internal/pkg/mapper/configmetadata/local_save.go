package configmetadata

import (
	"sort"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// MapBeforeLocalSave - store config metadata to manifest.
func (m *configMetadataMapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	manifest, ok := recipe.ObjectManifest.(*model.ConfigManifest)
	if !ok {
		return nil
	}

	config, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	if config.Metadata != nil {
		ordered := orderedmap.New()
		for key, val := range config.Metadata {
			ordered.Set(key, val)
		}
		ordered.SortKeys(sort.Strings)
		manifest.SetMetadata(ordered)
		recipe.ChangedFields.Remove(`metadata`)
	}
	return nil
}
