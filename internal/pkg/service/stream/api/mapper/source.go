package mapper

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/receive/receivectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func (m *Mapper) NewSourceEntity(parent key.BranchKey, payload *api.CreateSourcePayload) (definition.Source, error) {
	entity := definition.Source{}
	entity.BranchKey = parent

	// Generate source ID from Name if not set
	if payload.SourceID == nil || len(*payload.SourceID) == 0 {
		entity.SourceID = key.SourceID(strhelper.NormalizeName(payload.Name))
	} else {
		entity.SourceID = key.SourceID(strhelper.NormalizeName(string(*payload.SourceID)))
	}

	// Name
	entity.Name = payload.Name

	// Description is optional
	if payload.Description != nil {
		entity.Description = *payload.Description
	}

	// Type
	entity.Type = payload.Type
	switch entity.Type {
	case definition.SourceTypeHTTP:
		entity.HTTP = &definition.HTTPSource{
			Secret: idgenerator.StreamHTTPSourceSecret(),
		}
	default:
		return definition.Source{}, svcerrors.NewBadRequestError(errors.Errorf(`unexpected "type" "%s"`, payload.Type.String()))
	}

	return entity, nil
}

func (m *Mapper) UpdateSourceEntity(entity definition.Source, payload *api.UpdateSourcePayload) (definition.Source, error) {
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
		// Type
		entity.Type = *payload.Type
		switch entity.Type {
		case definition.SourceTypeHTTP:
			if entity.HTTP == nil {
				entity.HTTP = &definition.HTTPSource{}
			}
			if entity.HTTP.Secret == "" {
				entity.HTTP.Secret = idgenerator.StreamHTTPSourceSecret()
			}
		default:
			return definition.Source{}, svcerrors.NewBadRequestError(errors.Errorf(`unexpected "type" "%s"`, payload.Type.String()))
		}
	}

	return entity, nil
}

func (m *Mapper) NewSourceResponse(entity definition.Source) *api.Source {
	out := &api.Source{
		ProjectID:   entity.ProjectID,
		BranchID:    entity.BranchID,
		SourceID:    entity.SourceID,
		Type:        entity.Type,
		Name:        entity.Name,
		Description: entity.Description,
		Version:     m.NewVersionResponse(entity.Version),
		Deleted:     m.NewDeletedResponse(entity.SoftDeletable),
		Disabled:    m.NewDisabledResponse(entity.Switchable),
	}

	if entity.Type == definition.SourceTypeHTTP {
		out.HTTP = &api.HTTPSource{
			URL: m.formatHTTPSourceURL(entity),
		}
	}

	return out
}

func (m *Mapper) NewTestResultResponse(sourceKey key.SourceKey, sinks []definition.Sink, receiveCtx *receivectx.Context) (*api.TestResult, error) {
	result := &api.TestResult{
		ProjectID: sourceKey.ProjectID,
		BranchID:  sourceKey.BranchID,
		SourceID:  sourceKey.SourceID,
	}

	renderer := column.NewRenderer()

	for _, sink := range sinks {
		row := &api.TestResultRow{}
		for _, c := range sink.Table.Mapping.Columns {
			csvValue, err := renderer.CSVValue(c, receiveCtx)
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

func (m *Mapper) NewSourcesResponse(
	ctx context.Context,
	k key.BranchKey,
	sinceId string,
	limit int,
	list func(...iterator.Option) iterator.DefinitionT[definition.Source],
) (*api.SourcesList, error) {
	sources, page, err := loadPage(ctx, sinceId, limit, etcd.SortAscend, list, m.NewSourceResponse)
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

func (m *Mapper) formatHTTPSourceURL(entity definition.Source) string {
	return m.httpSourcePublicURL.
		JoinPath("projects", entity.ProjectID.String(), "sources", entity.SourceID.String(), entity.HTTP.Secret).
		String()
}
