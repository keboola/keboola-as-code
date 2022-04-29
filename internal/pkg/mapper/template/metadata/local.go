package metadata

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *metadataMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	if config, ok := recipe.Object.(*model.Config); ok {
		config.Metadata.SetRepository(m.templateRef.Repository().Name)
		config.Metadata.SetTemplateId(m.templateRef.TemplateId())
		config.Metadata.SetInstanceId(m.instanceId)
	}
	return nil
}
