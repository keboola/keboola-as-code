package mapper

import (
	"github.com/keboola/go-client/pkg/keboola"

	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func (m *Mapper) NewSinkEntity(parent key.SourceKey, payload *api.CreateSinkPayload) (entity definition.Sink, err error) {
	entity.SourceKey = parent

	// Generate source ID from Name if not set
	if payload.SinkID == nil || len(*payload.SinkID) == 0 {
		entity.SinkID = key.SinkID(strhelper.NormalizeName(payload.Name))
	} else {
		entity.SinkID = key.SinkID(strhelper.NormalizeName(string(*payload.SinkID)))
	}

	// Name
	entity.Name = payload.Name

	// Description is optional
	if payload.Description != nil {
		entity.Description = *payload.Description
	}

	// Sink type
	entity.Type = payload.Type
	switch entity.Type {
	case definition.SinkTypeTable:
		if tableEntity, err := m.newTableSinkEntity(payload.Table); err == nil {
			entity.Table = &tableEntity
		} else {
			return definition.Sink{}, err
		}
	default:
		return definition.Sink{}, svcerrors.NewBadRequestError(errors.Errorf(`unexpected "type" "%s"`, payload.Type.String()))
	}

	return entity, nil
}

func (m *Mapper) newTableSinkEntity(payload *api.TableSink) (entity definition.TableSink, err error) {
	// User has to specify table definition
	if payload == nil {
		return definition.TableSink{}, errors.Errorf(`"table" must be configured for the "%s" sink type`, definition.SinkTypeTable)
	}

	// Common table mapping
	entity.Mapping, err = m.newTableSinkMappingEntity(payload.Mapping)
	if err != nil {
		return definition.TableSink{}, err
	}

	// Table type
	entity.Type = payload.Type
	switch entity.Type {
	case definition.TableTypeKeboola:
		// Keboola table
		entity.Keboola = &definition.KeboolaTable{}

		// TableID
		if tableID, err := keboola.ParseTableID(string(payload.TableID)); err == nil {
			entity.Keboola.TableID = tableID
		} else {
			return definition.TableSink{}, svcerrors.NewBadRequestError(errors.Errorf(`invalid "tableId" value "%s": %w`, payload.TableID, err))
		}
	default:
		return definition.TableSink{}, svcerrors.NewBadRequestError(errors.Errorf(`unexpected "type" "%s"`, payload.Type.String()))
	}

	return entity, err
}

func (m *Mapper) newTableSinkMappingEntity(payload *api.TableMapping) (entity table.Mapping, err error) {
	// User has to specify table mapping definition
	if payload == nil {
		return table.Mapping{}, errors.Errorf(`"table.mapping" must be configured for the "%s" sink type`, definition.SinkTypeTable)
	}

	// Columns
	for _, columnPayload := range payload.Columns {
		columnEntity, err := column.MakeColumn(columnPayload.Type, columnPayload.Name, columnPayload.PrimaryKey)
		if err != nil {
			return table.Mapping{}, err
		}

		// Template column
		if tmplColumn, ok := columnEntity.(column.Template); ok {
			if columnPayload.Template == nil {
				return table.Mapping{}, svcerrors.NewBadRequestError(errors.Errorf(`column "%s" is missing template`, columnPayload.Name))
			}

			if err := m.jsonnetValidator.Validate(columnPayload.Template.Content); err != nil {
				return table.Mapping{}, svcerrors.NewBadRequestError(errors.Errorf(`column "%s" template is invalid: %w`, columnPayload.Name, err))
			}

			tmplColumn.Language = columnPayload.Template.Language
			tmplColumn.Content = columnPayload.Template.Content
			columnEntity = tmplColumn
		}

		entity.Columns = append(entity.Columns, columnEntity)
	}

	return entity, nil
}
