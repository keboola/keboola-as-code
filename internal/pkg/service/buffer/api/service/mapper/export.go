package mapper

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func (m Mapper) ExportsPayload(exports []model.Export) []*buffer.Export {
	out := make([]*buffer.Export, 0, len(exports))
	for _, data := range exports {
		out = append(out, m.ExportPayload(data))
	}
	return out
}

func (m Mapper) ExportPayload(model model.Export) *buffer.Export {
	mapping := m.MappingPayload(model.Mapping)
	conditions := &buffer.Conditions{
		Count: int(model.ImportConditions.Count),
		Size:  model.ImportConditions.Size.String(),
		Time:  model.ImportConditions.Time.String(),
	}
	return &buffer.Export{
		ID:         model.ExportID,
		ReceiverID: model.ReceiverID,
		Name:       model.Name,
		Mapping:    &mapping,
		Conditions: conditions,
	}
}

func (m Mapper) CreateExportModel(ctx context.Context, receiverKey key.ReceiverKey, payload buffer.CreateExportData) (r model.Export, err error) {
	export, err := m.createExportBaseModel(ctx, receiverKey, payload)
	if err != nil {
		return model.Export{}, err
	}
	mapping, err := m.CreateMappingModel(export.ExportKey, 1, *payload.Mapping)
	if err != nil {
		return model.Export{}, err
	}

	return model.Export{
		ExportBase: export,
		Mapping:    mapping,
	}, nil
}

func (m Mapper) UpdateExportModel(ctx context.Context, export *model.Export, payload *buffer.UpdateExportPayload) error {
	if payload.Name != nil {
		export.Name = *payload.Name
	}

	if payload.Mapping != nil {
		newMapping, err := m.CreateMappingModel(export.ExportKey, export.Mapping.RevisionID+1, *payload.Mapping)
		if err != nil {
			return err
		}
		export.Mapping = newMapping
	}

	if payload.Conditions != nil {
		newConditions, err := m.ConditionsModel(ctx, payload.Conditions)
		if err != nil {
			return err
		}
		export.ImportConditions = newConditions
	}

	return nil
}

func (m Mapper) createExportBaseModel(ctx context.Context, receiverKey key.ReceiverKey, payload buffer.CreateExportData) (r model.ExportBase, err error) {
	name := payload.Name

	// Generate export ID from Name if needed
	var id key.ExportID
	if payload.ID != nil && len(*payload.ID) != 0 {
		id = *payload.ID
	} else {
		id = key.ExportID(strhelper.NormalizeName(name))
	}

	// Handle conditions with defaults
	conditions, err := m.ConditionsModel(ctx, payload.Conditions)
	if err != nil {
		return model.ExportBase{}, err
	}

	return model.ExportBase{
		ExportKey: key.ExportKey{
			ReceiverKey: receiverKey,
			ExportID:    id,
		},
		Name:             name,
		ImportConditions: conditions,
	}, nil
}
