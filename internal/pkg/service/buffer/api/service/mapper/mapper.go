package mapper

import (
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type Mapper struct {
	bufferAPIHost string
}

func NewMapper(bufferAPIHost string) Mapper {
	return Mapper{bufferAPIHost}
}

func (m Mapper) ReceiverPayloadFromModel(model model.Receiver) buffer.Receiver {
	url := formatReceiverURL(m.bufferAPIHost, model.ReceiverKey, model.Secret)
	exports := make([]*buffer.Export, 0, len(model.Exports))
	for _, exportData := range model.Exports {
		export := m.ExportPayloadFromModel(exportData)
		exports = append(exports, &export)
	}

	return buffer.Receiver{
		ID:      buffer.ReceiverID(model.ReceiverID),
		URL:     url,
		Name:    model.Name,
		Exports: exports,
	}
}

func (m Mapper) UpdateReceiverFromPayload(receiver model.Receiver, payload buffer.UpdateReceiverPayload) (r model.Receiver, err error) {
	if payload.Name != nil {
		receiver.Name = *payload.Name
	}

	return receiver, nil
}

func (m Mapper) ExportPayloadFromModel(model model.Export) buffer.Export {
	mapping := m.MappingPayloadFromModel(model.Mapping)
	conditions := &buffer.Conditions{
		Count: model.ImportConditions.Count,
		Size:  model.ImportConditions.Size.String(),
		Time:  model.ImportConditions.Time.String(),
	}
	return buffer.Export{
		ID:         buffer.ExportID(model.ExportID),
		ReceiverID: buffer.ReceiverID(model.ReceiverID),
		Name:       model.Name,
		Mapping:    &mapping,
		Conditions: conditions,
	}
}

func (m Mapper) UpdateExportFromPayload(export model.Export, payload buffer.UpdateExportPayload) (r model.Export, err error) {
	if payload.Name != nil {
		export.Name = *payload.Name
	}

	if payload.Mapping != nil {
		newMapping, err := m.MappingModelFromPayload(export.ExportKey, export.Mapping.RevisionID+1, *payload.Mapping)
		if err != nil {
			return model.Export{}, err
		}
		export.Mapping = newMapping
	}

	if payload.Conditions != nil {
		newConditions, err := m.ConditionsFromPayload(payload.Conditions)
		if err != nil {
			return model.Export{}, err
		}
		export.ImportConditions = newConditions
	}

	return export, nil
}

func (m Mapper) MappingPayloadFromModel(model model.Mapping) buffer.Mapping {
	columns := make([]*buffer.Column, 0, len(model.Columns))
	for _, input := range model.Columns {
		output := &buffer.Column{
			Type: input.ColumnType(),
			Name: input.ColumnName(),
		}

		if v, ok := input.(column.Template); ok {
			output.Template = &buffer.Template{
				Language:               v.Language,
				UndefinedValueStrategy: v.UndefinedValueStrategy,
				Content:                v.Content,
			}
		}

		columns = append(columns, output)
	}
	return buffer.Mapping{
		TableID:     model.TableID.String(),
		Incremental: &model.Incremental,
		Columns:     columns,
	}
}

func (m Mapper) ReceiverModelFromPayload(projectID key.ProjectID, secret string, payload buffer.CreateReceiverPayload) (r model.Receiver, err error) {
	receiverBase := m.ReceiverBaseFromPayload(projectID, secret, payload)

	exports := make([]model.Export, 0, len(payload.Exports))
	for _, exportData := range payload.Exports {
		export, err := m.ExportBaseFromPayload(receiverBase.ReceiverKey, *exportData)
		if err != nil {
			return model.Receiver{}, err
		}

		mapping, err := m.MappingModelFromPayload(export.ExportKey, 1, *exportData.Mapping)
		if err != nil {
			return model.Receiver{}, err
		}

		// Persist export
		exports = append(exports, model.Export{
			ExportBase: export,
			Mapping:    mapping,
		})
	}

	return model.Receiver{
		ReceiverBase: receiverBase,
		Exports:      exports,
	}, nil
}

func (m Mapper) ReceiverBaseFromPayload(projectID key.ProjectID, secret string, payload buffer.CreateReceiverPayload) model.ReceiverBase {
	name := payload.Name

	// Generate receiver ID from Name if needed
	var id key.ReceiverID
	if payload.ID != nil && len(*payload.ID) != 0 {
		id = key.ReceiverID(strhelper.NormalizeName(string(*payload.ID)))
	} else {
		id = key.ReceiverID(strhelper.NormalizeName(name))
	}

	return model.ReceiverBase{
		ReceiverKey: key.ReceiverKey{
			ProjectID:  projectID,
			ReceiverID: id,
		},
		Name:   name,
		Secret: secret,
	}
}

func (m Mapper) ExportModelFromPayload(receiverKey key.ReceiverKey, payload buffer.CreateExportData) (r model.Export, err error) {
	export, err := m.ExportBaseFromPayload(receiverKey, payload)
	if err != nil {
		return model.Export{}, err
	}
	mapping, err := m.MappingModelFromPayload(export.ExportKey, 1, *payload.Mapping)
	if err != nil {
		return model.Export{}, err
	}

	return model.Export{
		ExportBase: export,
		Mapping:    mapping,
	}, nil
}

func (m Mapper) ExportBaseFromPayload(receiverKey key.ReceiverKey, payload buffer.CreateExportData) (r model.ExportBase, err error) {
	name := payload.Name

	// Generate export ID from Name if needed
	var id key.ExportID
	if payload.ID != nil && len(*payload.ID) != 0 {
		id = key.ExportID(*payload.ID)
	} else {
		id = key.ExportID(strhelper.NormalizeName(name))
	}

	// Handle conditions with defaults
	conditions, err := m.ConditionsFromPayload(payload.Conditions)
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

func (m Mapper) MappingModelFromPayload(exportKey key.ExportKey, revisionID key.RevisionID, payload buffer.Mapping) (model.Mapping, error) {
	// mapping
	tableID, err := model.ParseTableID(payload.TableID)
	if err != nil {
		return model.Mapping{}, err
	}
	columns := make([]column.Column, 0, len(payload.Columns))
	for _, data := range payload.Columns {
		c, err := column.MakeColumn(data.Type, data.Name)
		if err != nil {
			return model.Mapping{}, err
		}
		if v, ok := c.(column.Template); ok {
			if data.Template == nil {
				return model.Mapping{}, serviceError.NewBadRequestError(errors.Errorf(`column "%s" is missing template`, data.Name))
			}
			v.Name = c.ColumnName()
			v.Language = data.Template.Language
			v.UndefinedValueStrategy = data.Template.UndefinedValueStrategy
			v.Content = data.Template.Content
			c = v
		}
		columns = append(columns, c)
	}

	return model.Mapping{
		MappingKey: key.MappingKey{
			ExportKey:  exportKey,
			RevisionID: revisionID,
		},
		TableID:     tableID,
		Incremental: payload.Incremental == nil || *payload.Incremental, // default true
		Columns:     columns,
	}, nil
}

func (m Mapper) ConditionsFromPayload(payload *buffer.Conditions) (r model.ImportConditions, err error) {
	conditions := model.DefaultConditions()
	if payload != nil {
		conditions.Count = payload.Count
		conditions.Size, err = datasize.ParseString(payload.Size)
		if err != nil {
			return model.ImportConditions{}, serviceError.NewBadRequestError(errors.Errorf(
				`value "%s" is not valid buffer size in bytes. Allowed units: B, kB, MB. For example: "5MB"`,
				payload.Size,
			))
		}
		conditions.Time, err = time.ParseDuration(payload.Time)
		if err != nil {
			return model.ImportConditions{}, serviceError.NewBadRequestError(errors.Errorf(
				`value "%s" is not valid time duration. Allowed units: s, m, h. For example: "30s"`,
				payload.Size,
			))
		}
	}
	return conditions, nil
}

func formatReceiverURL(bufferAPIHost string, k key.ReceiverKey, secret string) string {
	return fmt.Sprintf("https://%s/v1/import/%s/%s/%s", bufferAPIHost, k.ProjectID.String(), k.ReceiverID.String(), secret)
}
