package metadata

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	templateMetadata "github.com/keboola/keboola-as-code/internal/pkg/template/metadata"
)

func (m *metadataMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	if config, ok := recipe.Object.(*model.Config); ok {
		metadata := templateMetadata.ConfigMetadata(config.Metadata)
		metadata.SetRepository(m.templateRef.Repository().Name)
		metadata.SetTemplateId(m.templateRef.TemplateId())
		metadata.SetInstanceId(m.instanceId)
	}
	return nil
}
