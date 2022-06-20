package configmetadata

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapAfterLocalLoad - load metadata from manifest to config.
func (m *configMetadataMapper) MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	manifest, ok := recipe.ObjectManifest.(*model.ConfigManifest)
	if !ok {
		return nil
	}

	config, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	config.Metadata = manifest.MetadataMap()
	return nil
}
