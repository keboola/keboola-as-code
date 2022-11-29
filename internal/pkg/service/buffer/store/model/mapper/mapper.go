package mapper

import (
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func ReceiverPayloadFromModel(bufferAPIHost string, model model.Receiver) buffer.Receiver {
	id := buffer.ReceiverID(model.ReceiverID)
	url := formatReceiverURL(bufferAPIHost, model.ProjectID, model.ReceiverID, model.Secret)
	exports := make([]*buffer.Export, 0, len(model.Exports))
	for _, exportData := range model.Exports {
		export := ExportPayloadFromModel(id, exportData)
		exports = append(exports, &export)
	}

	return buffer.Receiver{
		ID:      id,
		URL:     url,
		Name:    model.Name,
		Exports: exports,
	}
}

func ExportPayloadFromModel(receiverID buffer.ReceiverID, model model.Export) buffer.Export {
	mapping := MappingPayloadFromModel(model.Mapping)
	conditions := &buffer.Conditions{
		Count: model.ImportConditions.Count,
		Size:  model.ImportConditions.Size.String(),
		Time:  model.ImportConditions.Time.String(),
	}
	return buffer.Export{
		ID:         buffer.ExportID(model.ExportID),
		ReceiverID: receiverID,
		Name:       model.Name,
		Mapping:    &mapping,
		Conditions: conditions,
	}
}

func MappingPayloadFromModel(model model.Mapping) buffer.Mapping {
	columns := make([]*buffer.Column, 0, len(model.Columns))
	for _, c := range model.Columns {
		var template *buffer.Template
		if v, ok := c.(column.Template); ok {
			template = &buffer.Template{
				Language:               v.Language,
				UndefinedValueStrategy: v.UndefinedValueStrategy,
				Content:                v.Content,
				DataType:               v.DataType,
			}
		}
		typ, _ := column.ColumnToType(c)
		columns = append(columns, &buffer.Column{
			Type:     typ,
			Template: template,
		})
	}
	return buffer.Mapping{
		TableID:     model.TableID.String(),
		Incremental: &model.Incremental,
		Columns:     columns,
	}
}

func ReceiverModelFromPayload(projectID int, payload buffer.CreateReceiverPayload) (r model.Receiver, err error) {
	receiverBase := ReceiverBaseFromPayload(projectID, payload)

	exports := make([]model.Export, 0, len(payload.Exports))
	for _, exportData := range payload.Exports {
		mapping, err := MappingFromPayload(*exportData.Mapping)
		if err != nil {
			return model.Receiver{}, err
		}

		export, err := ExportBaseFromPayload(receiverBase.ReceiverKey, *exportData)
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

func ReceiverBaseFromPayload(projectID int, payload buffer.CreateReceiverPayload) model.ReceiverBase {
	name := payload.Name

	// Generate receiver ID from Name if needed
	var id string
	if payload.ID != nil && len(*payload.ID) != 0 {
		id = strhelper.NormalizeName(string(*payload.ID))
	} else {
		id = strhelper.NormalizeName(name)
	}

	// Generate Secret
	secret := idgenerator.ReceiverSecret()

	return model.ReceiverBase{
		ReceiverKey: key.ReceiverKey{
			ProjectID:  projectID,
			ReceiverID: id,
		},
		Name:   name,
		Secret: secret,
	}
}

func ExportBaseFromPayload(receiverKey key.ReceiverKey, payload buffer.CreateExportData) (r model.ExportBase, err error) {
	name := payload.Name

	// Generate export ID from Name if needed
	var id string
	if payload.ID != nil && len(*payload.ID) != 0 {
		id = string(*payload.ID)
	} else {
		id = strhelper.NormalizeName(name)
	}

	// Handle conditions with defaults
	conditions := model.DefaultConditions()
	if payload.Conditions != nil {
		conditions.Count = payload.Conditions.Count
		conditions.Size, err = datasize.ParseString(payload.Conditions.Size)
		if err != nil {
			return model.ExportBase{}, serviceError.NewBadRequestError(errors.Errorf(
				`value "%s" is not valid buffer size in bytes. Allowed units: B, kB, MB. For example: "5MB"`,
				payload.Conditions.Size,
			))
		}
		conditions.Time, err = time.ParseDuration(payload.Conditions.Time)
		if err != nil {
			return model.ExportBase{}, serviceError.NewBadRequestError(errors.Errorf(
				`value "%s" is not valid time duration. Allowed units: s, m, h. For example: "30s"`,
				payload.Conditions.Size,
			))
		}
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

func MappingFromPayload(payload buffer.Mapping) (model.Mapping, error) {
	// mapping
	tableID, err := model.ParseTableID(payload.TableID)
	if err != nil {
		return model.Mapping{}, err
	}
	columns := make([]column.Column, 0, len(payload.Columns))
	for _, columnData := range payload.Columns {
		c, err := column.TypeToColumn(columnData.Type)
		if err != nil {
			return model.Mapping{}, err
		}
		if template, ok := c.(column.Template); ok {
			if columnData.Template == nil {
				return model.Mapping{}, serviceError.NewBadRequestError(errors.Errorf("missing template column data"))
			}
			template.Language = columnData.Template.Language
			template.UndefinedValueStrategy = columnData.Template.UndefinedValueStrategy
			template.DataType = columnData.Template.DataType
			template.Content = columnData.Template.Content
			c = template
		}
		columns = append(columns, c)
	}

	return model.Mapping{
		TableID:     tableID,
		Incremental: payload.Incremental == nil || *payload.Incremental, // default true
		Columns:     columns,
	}, nil
}

func formatReceiverURL(bufferAPIHost string, projectID int, receiverID string, secret string) string {
	return fmt.Sprintf("https://%s/v1/import/%d/%s/%s", bufferAPIHost, projectID, receiverID, secret)
}
