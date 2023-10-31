package mapper

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m Mapper) MappingPayload(model model.Mapping) buffer.Mapping {
	columns := make([]*buffer.Column, 0, len(model.Columns))
	for _, input := range model.Columns {
		output := &buffer.Column{
			Type:       input.ColumnType(),
			Name:       input.ColumnName(),
			PrimaryKey: input.IsPrimaryKey(),
		}

		if v, ok := input.(column.Template); ok {
			output.Template = &buffer.Template{
				Language: v.Language,
				Content:  v.Content,
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

func (m Mapper) CreateMappingModel(exportKey key.ExportKey, revisionID key.RevisionID, payload buffer.Mapping) (model.Mapping, error) {
	// mapping
	tableID, err := keboola.ParseTableID(payload.TableID)
	if err != nil {
		return model.Mapping{}, err
	}
	columns := make([]column.Column, 0, len(payload.Columns))
	for _, data := range payload.Columns {
		c, err := column.MakeColumn(data.Type, data.Name, data.PrimaryKey)
		if err != nil {
			return model.Mapping{}, err
		}
		if v, ok := c.(column.Template); ok {
			if data.Template == nil {
				return model.Mapping{}, serviceError.NewBadRequestError(errors.Errorf(`column "%s" is missing template`, data.Name))
			}
			v.Name = c.ColumnName()
			v.Language = data.Template.Language
			if err := m.templateValidator.Validate(data.Template.Content); err != nil {
				return model.Mapping{}, serviceError.NewBadRequestError(errors.Errorf(`column "%s" template is invalid: %w`, data.Name, err))
			}
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
