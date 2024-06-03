package service

import (
	"context"

	"golang.org/x/exp/maps"

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
	sourceKeys := make(map[key.SourceKey]int)
	for i, source := range response.Sources {
		sourceKey := key.SourceKey{
			BranchKey: d.BranchKey(),
			SourceID:  source.SourceID,
		}
		sourceKeys[sourceKey] = i
	}

	sourcesWithSinks, err := d.AggregationRepository().GetSourcesWithSinksAndStatistics(ctx, maps.Keys(sourceKeys))
	if err != nil {
		return err
	}

	for _, sourceWithSinks := range sourcesWithSinks {
		for _, sink := range sourceWithSinks.Sinks {
			sink, err := s.mapper.NewAggregationSinkResponse(*sink)
			if err != nil {
				return err
			}

			source := response.Sources[sourceKeys[sourceWithSinks.SourceKey]]
			source.Sinks = append(source.Sinks, sink)
		}
	}

	return nil
}
