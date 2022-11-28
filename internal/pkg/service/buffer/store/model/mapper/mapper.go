package mapper

import (
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func ReceiverFromPayload(projectID int, payload *buffer.CreateReceiverPayload) (r *model.Receiver, err error) {
	exports := make([]*model.Export, 0, len(payload.Exports))
	for _, exportData := range payload.Exports {
		mapping, err := MappingFromPayload(exportData.Mapping)
		if err != nil {
			return nil, err
		}

		export, err := ExportBaseFromPayload(exportData)
		if err != nil {
			return nil, err
		}

		// Persist export
		exports = append(exports, &model.Export{
			ExportBase: export,
			Mapping:    mapping,
		})
	}

	receiverBase := ReceiverBaseFromPayload(projectID, payload)

	return &model.Receiver{
		ReceiverBase: receiverBase,
		Exports:      exports,
	}, nil
}

func ReceiverBaseFromPayload(projectID int, payload *buffer.CreateReceiverPayload) *model.ReceiverBase {
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

	return &model.ReceiverBase{
		ID:        id,
		ProjectID: projectID,
		Name:      name,
		Secret:    secret,
	}
}

func ExportBaseFromPayload(payload *buffer.CreateExportData) (r *model.ExportBase, err error) {
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
			return nil, serviceError.NewBadRequestError(errors.Errorf(
				`value "%s" is not valid buffer size in bytes. Allowed units: B, kB, MB. For example: "5MB"`,
				payload.Conditions.Size,
			))
		}
		conditions.Time, err = time.ParseDuration(payload.Conditions.Time)
		if err != nil {
			return nil, serviceError.NewBadRequestError(errors.Errorf(
				`value "%s" is not valid time duration. Allowed units: s, m, h. For example: "30s"`,
				payload.Conditions.Size,
			))
		}
	}

	return &model.ExportBase{
		ID:               id,
		Name:             name,
		ImportConditions: conditions,
	}, nil
}

func MappingFromPayload(payload *buffer.Mapping) (*model.Mapping, error) {
	// mapping
	tableID, err := model.ParseTableID(payload.TableID)
	if err != nil {
		return nil, err
	}
	columns := make([]column.Column, 0, len(payload.Columns))
	for _, columnData := range payload.Columns {
		c, err := column.TypeToColumn(columnData.Type)
		if err != nil {
			return nil, err
		}
		if template, ok := c.(column.Template); ok {
			if columnData.Template == nil {
				return nil, serviceError.NewBadRequestError(errors.Errorf("missing template column data"))
			}
			template.Language = columnData.Template.Language
			template.UndefinedValueStrategy = columnData.Template.UndefinedValueStrategy
			template.DataType = columnData.Template.DataType
			template.Content = columnData.Template.Content
			c = template
		}
		columns = append(columns, c)
	}

	return &model.Mapping{
		TableID:     tableID,
		Incremental: payload.Incremental == nil || *payload.Incremental, // default true
		Columns:     columns,
	}, nil
}
