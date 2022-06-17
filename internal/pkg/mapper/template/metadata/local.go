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
			// Store instance metadata
			config.Metadata.SetTemplateInstance(m.templateRef.Repository().Name, m.templateRef.TemplateId(), m.instanceId)
			// Store original object ID
			if idInTemplate, found := m.objectIds.IdInTemplate(v.Id); found {
				config.Metadata.SetConfigTemplateId(idInTemplate.(storageapi.ConfigID))
			}
			// Store inputs usage
			if inputsUsage, ok := m.inputsUsage.Values[config.Key()]; ok {
				for _, item := range inputsUsage {
					config.Metadata.AddInputUsage(item.Name, item.JsonKey)
				}
			}
		case *model.ConfigRowState:
			// Config row has no metadata support, so row templateId -> projectId pairs are stored in config metadata.
			config := m.state.MustGet(v.ConfigKey()).(*model.ConfigState).Local
			// Store original object ID
			if idInTemplate, found := m.objectIds.IdInTemplate(v.Id); found {
				config.Metadata.AddRowTemplateId(v.Id, idInTemplate.(storageapi.RowID))
			}
			// Store inputs usage
			if inputsUsage, ok := m.inputsUsage.Values[v.Key()]; ok {
				for _, item := range inputsUsage {
					config.Metadata.AddRowInputUsage(v.Id, item.Name, item.JsonKey)
				}
			}
		}
	}

	return nil
}
