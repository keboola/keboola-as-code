package configmetadata

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeLocalSave - store config metadata to manifest.
func (m *configMetadataMapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	manifest, ok := recipe.ObjectManifest.(*model.ConfigManifest)
	if !ok {
		return nil
	}

	config, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	manifest.Metadata = config.MetadataOrderedMap()
	if len(manifest.Metadata.Keys()) == 0 {
		manifest.Metadata = nil
	}
	recipe.ChangedFields.Remove(`metadata`)
	return nil
}
