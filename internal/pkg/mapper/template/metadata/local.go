package metadata

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *metadataMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	if config, ok := recipe.Object.(*model.Config); ok {
		config.Metadata.SetTemplateInstance(m.templateRef.Repository().Name, m.templateRef.TemplateId(), m.instanceId)
	}
	return nil
}
