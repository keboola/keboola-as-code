package metadata

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *metadataMapper) AfterLocalOperation(_ context.Context, changes *model.LocalChanges) error {
	for _, objectState := range changes.Loaded() {
		if !objectState.HasLocalState() {
			continue
		}

		switch v := objectState.(type) {
		case *model.ConfigState:
			config := v.Local

			// Skip shared code config.
			// It may already exist, or it was created by the template,
			// it doesn't matter, it is not related to the template, it is container for shared codes.
			if config.SharedCode != nil {
				continue
			}

			// Store instance metadata
			config.Metadata.SetTemplateInstance(m.templateRef.Repository().Name, m.templateRef.TemplateID(), m.instanceID)
			// Store original object ID
			if idInTemplate, found := m.objectIds.IDInTemplate(v.ID); found {
				config.Metadata.SetConfigTemplateID(idInTemplate.(keboola.ConfigID))
			}
			// Store inputs usage
			if inputsUsage, ok := m.inputsUsage.Values[config.Key()]; ok {
				for _, item := range inputsUsage {
					config.Metadata.AddInputUsage(item.Name, item.JSONKey, item.ObjectKeys)
				}
			}
		case *model.ConfigRowState:
			// Config row has no metadata support, so row templateId -> projectId pairs are stored in config metadata.
			config := m.state.MustGet(v.ConfigKey()).(*model.ConfigState).Local
			// Store original object ID
			if idInTemplate, found := m.objectIds.IDInTemplate(v.ID); found {
				config.Metadata.AddRowTemplateID(v.ID, idInTemplate.(keboola.RowID))
			}
			// Store inputs usage
			if inputsUsage, ok := m.inputsUsage.Values[v.Key()]; ok {
				for _, item := range inputsUsage {
					config.Metadata.AddRowInputUsage(v.ID, item.Name, item.JSONKey, item.ObjectKeys)
				}
			}
		}
	}

	return nil
}
