package mapper

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *Mapper) NewSourceResponse(entity definition.Source) (*api.Source, error) {
	out := &api.Source{
		ProjectID:   entity.ProjectID,
		BranchID:    entity.BranchID,
		SourceID:    entity.SourceID,
		Type:        entity.Type,
		Name:        entity.Name,
		Description: entity.Description,
		Created:     m.NewCreatedResponse(entity.Created),
		Version:     m.NewVersionResponse(entity.Version),
		Deleted:     m.NewDeletedResponse(entity.SoftDeletable),
		Disabled:    m.NewDisabledResponse(entity.Switchable),
	}

	// Type
	switch out.Type {
	case definition.SourceTypeHTTP:
		out.HTTP = &api.HTTPSource{
			URL: m.formatHTTPSourceURL(entity),
		}
	default:
		return nil, svcerrors.NewBadRequestError(errors.Errorf(`unexpected "type" "%s"`, out.Type.String()))
	}

	return out, nil
}

func (m *Mapper) NewSourcesResponse(
	ctx context.Context,
	k key.BranchKey,
	afterId string,
	limit int,
	list func(...iterator.Option) iterator.DefinitionT[definition.Source],
) (*api.SourcesList, error) {
	sources, page, err := loadPage(ctx, afterId, limit, etcd.SortAscend, list, m.NewSourceResponse)
	if err != nil {
		return nil, err
	}

	return &api.SourcesList{
		ProjectID: k.ProjectID,
		BranchID:  k.BranchID,
		Page:      page,
		Sources:   sources,
	}, nil
}

func (m *Mapper) NewTestResultResponse(sourceKey key.SourceKey, sinks []definition.Sink, recordCtx recordctx.Context) (*api.TestResult, error) {
	result := &api.TestResult{
		ProjectID: sourceKey.ProjectID,
		BranchID:  sourceKey.BranchID,
		SourceID:  sourceKey.SourceID,
	}

	renderer := column.NewRenderer()

	for _, sink := range sinks {
		row := &api.TestResultRow{}
		for _, c := range sink.Table.Mapping.Columns {
			csvValue, err := renderer.CSVValue(c, recordCtx)
			if err != nil {
				return nil, err
			}

			row.Columns = append(row.Columns, &api.TestResultColumn{
				Name:  c.ColumnName(),
				Value: csvValue,
			})
		}

		result.Tables = append(result.Tables, &api.TestResultTable{
			SinkID:  sink.SinkID,
			TableID: api.TableID(sink.Table.Keboola.TableID.String()),
			Rows:    []*api.TestResultRow{row},
		})
	}

	return result, nil
}

func (m *Mapper) formatHTTPSourceURL(entity definition.Source) string {
	return m.httpSourcePublicURL.
		JoinPath("projects", entity.ProjectID.String(), "sources", entity.SourceID.String(), entity.HTTP.Secret).
		String()
}
