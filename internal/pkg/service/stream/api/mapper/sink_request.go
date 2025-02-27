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
		if tableEntity, err := m.newTableSinkEntity(payload); err == nil {
			entity.Table = &tableEntity
		} else {
			return definition.Sink{}, err
		}
	default:
		return definition.Sink{}, svcerrors.NewBadRequestError(errors.Errorf(`unexpected "type" "%s"`, payload.Type.String()))
	}

	return entity, nil
}

func (m *Mapper) UpdateSinkEntity(entity definition.Sink, payload *api.UpdateSinkPayload) (definition.Sink, error) {
	// Name
	if payload.Name != nil {
		entity.Name = *payload.Name
	}

	// Description
	if payload.Description != nil {
		entity.Description = *payload.Description
	}

	// Type
	if payload.Type != nil {
		entity.Type = *payload.Type
	}

	// Type specific updates
	switch entity.Type {
	case definition.SinkTypeTable:
		if entity.Table == nil {
			entity.Table = &definition.TableSink{}
		}
		if payload.Table != nil {
			if err := m.updateTableSinkEntity(entity.Table, payload); err != nil {
				return definition.Sink{}, err
			}
		}
	default:
		return definition.Sink{}, svcerrors.NewBadRequestError(errors.Errorf(`unexpected "type" "%s"`, payload.Type.String()))
	}

	return entity, nil
}

func (m *Mapper) newTableSinkEntity(payload *api.CreateSinkPayload) (entity definition.TableSink, err error) {
	// User has to specify table definition
	if payload.Table == nil {
		return definition.TableSink{}, svcerrors.NewBadRequestError(errors.Errorf(`"table" must be configured for the "%s" sink type`, definition.SinkTypeTable))
	}

	// Common table mapping
	entity.Mapping, err = m.newTableSinkMappingEntity(payload.Table.Mapping)
	if err != nil {
		return definition.TableSink{}, err
	}

	// Table type
	entity.Type = payload.Table.Type

	// Table type specific fields
	switch entity.Type {
	case definition.TableTypeKeboola:
		// Keboola table
		entity.Keboola = &definition.KeboolaTable{}

		// TableID
		if tableID, err := keboola.ParseTableID(string(payload.Table.TableID)); err == nil {
			entity.Keboola.TableID = tableID
		} else {
			return definition.TableSink{}, svcerrors.NewBadRequestError(errors.Errorf(`invalid "tableId" value "%s": %w`, payload.Table.TableID, err))
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

	vm := m.jsonnetPool.Get()
	defer m.jsonnetPool.Put(vm)

	// Columns
	for _, columnPayload := range payload.Columns {
		columnEntity, err := column.MakeColumn(columnPayload.Type, columnPayload.Name, false)
		if err != nil {
			return table.Mapping{}, err
		}

		// Path column
		if pathColumn, ok := columnEntity.(column.Path); ok {
			if columnPayload.Path == nil {
				return table.Mapping{}, svcerrors.NewBadRequestError(errors.Errorf(`column "%s" is missing path`, columnPayload.Name))
			}

			pathColumn.Path = *columnPayload.Path
			pathColumn.RawString = columnPayload.RawString != nil && *columnPayload.RawString
			pathColumn.DefaultValue = columnPayload.DefaultValue
			columnEntity = pathColumn
		}

		// Template column
		if tmplColumn, ok := columnEntity.(column.Template); ok {
			if columnPayload.Template == nil {
				return table.Mapping{}, svcerrors.NewBadRequestError(errors.Errorf(`column "%s" is missing template`, columnPayload.Name))
			}

			if err := vm.Validate(columnPayload.Template.Content); err != nil {
				return table.Mapping{}, svcerrors.NewBadRequestError(errors.Errorf(`column "%s" template is invalid: %w`, columnPayload.Name, err))
			}

			tmplColumn.Template.Language = columnPayload.Template.Language
			tmplColumn.Template.Content = columnPayload.Template.Content
			tmplColumn.RawString = columnPayload.RawString != nil && *columnPayload.RawString
			columnEntity = tmplColumn
		}

		entity.Columns = append(entity.Columns, columnEntity)
	}

	return entity, nil
}

func (m *Mapper) updateTableSinkEntity(entity *definition.TableSink, payload *api.UpdateSinkPayload) (err error) {
	// Common table mapping
	if payload.Table.Mapping != nil {
		entity.Mapping, err = m.newTableSinkMappingEntity(payload.Table.Mapping)
		if err != nil {
			return err
		}
	}

	// Table type
	if payload.Table.Type != nil {
		entity.Type = *payload.Table.Type
	}

	// Table type specific fields
	switch entity.Type {
	case definition.TableTypeKeboola:
		// Keboola table
		entity.Keboola = &definition.KeboolaTable{}

		// TableID
		if payload.Table.TableID != nil {
			if tableID, err := keboola.ParseTableID(string(*payload.Table.TableID)); err == nil {
				entity.Keboola.TableID = tableID
			} else {
				return svcerrors.NewBadRequestError(errors.Errorf(`invalid "tableId" value "%s": %w`, *payload.Table.TableID, err))
			}
		}
	default:
		return svcerrors.NewBadRequestError(errors.Errorf(`unexpected "type" "%s"`, payload.Type.String()))
	}

	return err
}
