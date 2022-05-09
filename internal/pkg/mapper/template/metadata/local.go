package metadata

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *metadataMapper) AfterLocalOperation(changes *model.LocalChanges) error {
	for _, objectState := range changes.Loaded() {
		if !objectState.HasLocalState() {
			continue
		}

		switch v := objectState.(type) {
		case *model.ConfigState:
			config := v.Local
			config.Metadata.SetTemplateInstance(m.templateRef.Repository().Name, m.templateRef.TemplateId(), m.instanceId)
			if idInTemplate, found := m.objectIds.IdInTemplate(v.Id); found {
				config.Metadata.SetConfigTemplateId(idInTemplate.(model.ConfigId))
			}
		case *model.ConfigRowState:
			// Config row has no metadata support, so row templateId -> projectId pairs are stored in config metadata.
			config := m.state.MustGet(v.ConfigKey()).(*model.ConfigState).Local
			if idInTemplate, found := m.objectIds.IdInTemplate(v.Id); found {
				config.Metadata.AddRowTemplateId(v.Id, idInTemplate.(model.RowId))
			}
		}
	}

	return nil
}
