package mapper

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *Mapper) NewSinkResponse(entity definition.Sink) (*api.Sink, error) {
	out := &api.Sink{
		ProjectID:   entity.ProjectID,
		BranchID:    entity.BranchID,
		SourceID:    entity.SourceID,
		SinkID:      entity.SinkID,
		Name:        entity.Name,
		Description: entity.Description,
		Created:     m.NewCreatedResponse(entity.Created),
		Version:     m.NewVersionResponse(entity.Version),
		Deleted:     m.NewDeletedResponse(entity.SoftDeletable),
		Disabled:    m.NewDisabledResponse(entity.Switchable),
	}

	// Type
	out.Type = entity.Type
	switch out.Type {
	case definition.SinkTypeTable:
		tableResponse, err := m.newTableSinkResponse(entity.Table)
		if err != nil {
			return nil, err
		}
		out.Table = &tableResponse
	default:
		return nil, svcerrors.NewBadRequestError(errors.Errorf(`unexpected "type" "%s"`, out.Type.String()))
	}

	return out, nil
}

func (m *Mapper) NewSinksResponse(
	ctx context.Context,
	k key.SourceKey,
	sinceId string,
	limit int,
	list func(...iterator.Option) iterator.DefinitionT[definition.Sink],
) (*api.SinksList, error) {
	sinks, page, err := loadPage(ctx, sinceId, limit, etcd.SortAscend, list, m.NewSinkResponse)
	if err != nil {
		return nil, err
	}

	return &api.SinksList{
		ProjectID: k.ProjectID,
		BranchID:  k.BranchID,
		SourceID:  k.SourceID,
		Page:      page,
		Sinks:     sinks,
	}, nil
}

func (m *Mapper) newTableSinkResponse(entity *definition.TableSink) (out api.TableSink, err error) {
	// Common table mapping
	mapping := m.newTableMappingResponse(entity.Mapping)
	out.Mapping = &mapping

	// Type
	out.Type = entity.Type
	switch out.Type {
	case definition.TableTypeKeboola:
		// Keboola table
		out.TableID = api.TableID(entity.Keboola.TableID.String())
	default:
		return api.TableSink{}, svcerrors.NewBadRequestError(errors.Errorf(`unexpected "table.type" "%s"`, out.Type.String()))
	}

	return out, nil
}

func (m *Mapper) newTableMappingResponse(entity table.Mapping) (out api.TableMapping) {
	out.Columns = make(api.TableColumns, 0, len(entity.Columns))
	for _, input := range entity.Columns {
		output := &api.TableColumn{
			Type:       input.ColumnType(),
			Name:       input.ColumnName(),
			PrimaryKey: input.IsPrimaryKey(),
		}

		if v, ok := input.(column.Template); ok {
			output.Template = &api.TableColumnTemplate{
				Language: v.Template.Language,
				Content:  v.Template.Content,
			}
		}

		out.Columns = append(out.Columns, output)
	}

	return out
}
