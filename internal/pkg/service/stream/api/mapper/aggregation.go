package mapper

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/aggregation/repository"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *Mapper) NewAggregationSourcesResponse(
	ctx context.Context,
	k key.BranchKey,
	sinceId string,
	limit int,
	list func(...iterator.Option) iterator.DefinitionT[definition.Source],
) (*api.AggregationSourcesResult, error) {
	sources, page, err := loadPage(ctx, sinceId, limit, etcd.SortAscend, list, m.NewAggregationSource)
	if err != nil {
		return nil, err
	}

	return &api.AggregationSourcesResult{
		ProjectID: k.ProjectID,
		BranchID:  k.BranchID,
		Page:      page,
		Sources:   sources,
	}, nil
}

func (m *Mapper) NewAggregationSource(entity definition.Source) (*api.AggregationSource, error) {
	out := &api.AggregationSource{
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

func (m *Mapper) NewAggregationSinkResponse(entity repository.SinkWithStatistics) (*api.AggregationSink, error) {
	out := &api.AggregationSink{
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

	if entity.Statistics.Total != nil {
		totals := m.NewSinkStatisticsTotalResponse(*entity.Statistics.Total)
		files := api.SinkFiles{}
		for _, file := range entity.Statistics.Files {
			sinkFile := m.NewSinkFile(*file.File)
			if file.Statistics != nil {
				sinkFile.Statistics = m.NewSinkFileStatistics(file.Statistics)
			}
			files = append(files, sinkFile)
		}
		out.Statistics = &api.AggregationStatistics{
			Total:  totals.Total,
			Levels: totals.Levels,
			Files:  files,
		}
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
