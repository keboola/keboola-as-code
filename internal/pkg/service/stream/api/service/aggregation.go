package service

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
)

func (s *service) AggregateSources(ctx context.Context, d dependencies.BranchRequestScope, payload *stream.AggregateSourcesPayload) (res *stream.AggregatedSourcesResult, err error) {
	list := func(opts ...iterator.Option) iterator.DefinitionT[definition.Source] {
		return s.definition.Source().List(d.BranchKey(), opts...)
	}

	response, err := s.mapper.NewAggregationSourcesResponse(ctx, d.BranchKey(), payload.SinceID, payload.Limit, list)
	if err != nil {
		return nil, err
	}

	err = s.addSinksToAggregationResponse(ctx, d, response)
	if err != nil {
		return nil, err
	}

	return response, err
}

func (s *service) addSinksToAggregationResponse(ctx context.Context, d dependencies.BranchRequestScope, response *stream.AggregatedSourcesResult) error {
	// Collect source IDs
	var sourceKeys []key.SourceKey
	for _, source := range response.Sources {
		sourceKey := key.SourceKey{
			BranchKey: d.BranchKey(),
			SourceID:  source.SourceID,
		}
		sourceKeys = append(sourceKeys, sourceKey)
	}

	// Get sinks for all the sources
	sourcesWithSinks, err := d.AggregationRepository().SourcesWithSinksAndStatistics(ctx, sourceKeys)
	if err != nil {
		return err
	}

	// Add sinks to response
	for _, source := range response.Sources {
		sourceKey := key.SourceKey{
			BranchKey: d.BranchKey(),
			SourceID:  source.SourceID,
		}

		sourceWithSinks, ok := sourcesWithSinks[sourceKey]
		if !ok {
			continue
		}

		for _, sink := range sourceWithSinks.Sinks {
			if sink == nil {
				continue
			}

			sinkResponse, err := s.mapper.NewAggregationSinkResponse(*sink)
			if err != nil {
				return err
			}

			source.Sinks = append(source.Sinks, sinkResponse)
		}
	}

	return nil
}
