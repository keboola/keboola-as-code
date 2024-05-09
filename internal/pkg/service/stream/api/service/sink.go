package service

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"golang.org/x/exp/maps"
)

func (s *service) CreateSink(_ context.Context, d dependencies.SourceRequestScope, payload *stream.CreateSinkPayload) (res *stream.Task, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) GetSink(context.Context, dependencies.SinkRequestScope, *stream.GetSinkPayload) (res *stream.Sink, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) ListSinks(context.Context, dependencies.SourceRequestScope, *stream.ListSinksPayload) (res *stream.SinksList, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSink(context.Context, dependencies.SinkRequestScope, *stream.UpdateSinkPayload) (res *stream.Task, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) DeleteSink(context.Context, dependencies.SinkRequestScope, *stream.DeleteSinkPayload) (err error) {
	return errors.NewNotImplementedError()
}

func (s *service) GetSinkSettings(context.Context, dependencies.SinkRequestScope, *stream.GetSinkSettingsPayload) (res *stream.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) UpdateSinkSettings(context.Context, dependencies.SinkRequestScope, *stream.UpdateSinkSettingsPayload) (res *stream.SettingsResult, err error) {
	return nil, errors.NewNotImplementedError()
}

func (s *service) SinkStatisticsTotal(ctx context.Context, d dependencies.SinkRequestScope, payload *stream.SinkStatisticsTotalPayload) (res *stream.SinkStatisticsTotalResult, err error) {
	err = s.repo.Sink().ExistsOrErr(d.SinkKey()).Do(ctx).Err()
	if err != nil {
		return nil, err
	}

	stats, err := d.StatisticsRepository().SinkStats(ctx, d.SinkKey())
	if err != nil {
		return nil, err
	}

	return s.mapper.NewSinkStatisticsTotalResponse(stats), nil
}

func (s *service) SinkStatisticsFiles(ctx context.Context, d dependencies.SinkRequestScope, payload *stream.SinkStatisticsFilesPayload) (res *stream.SinkStatisticsFilesResult, err error) {
	filesMap := make(map[model.FileID]*stream.SinkFile)

	// TODO: I'm surprised that I'm not passing ctx anywhere here. Am I doing it correctly?
	d.StorageRepository().File().ListRecentIn(d.SinkKey()).ForEach(func(value model.File, header *iterator.Header) error {
		filesMap[value.FileID] = &stream.SinkFile{
			State:       value.State,
			OpenedAt:    value.OpenedAt().String(),
			ClosingAt:   timeToString(value.ClosingAt),
			ImportingAt: timeToString(value.ImportingAt),
			ImportedAt:  timeToString(value.ImportedAt),
		}
		return nil
	})



	res = &stream.SinkStatisticsFilesResult{
		Files: maps.Values(filesMap),
	}

	return res, nil
}

func timeToString(time *utctime.UTCTime) *string {
	if time == nil {
		return nil
	}

	str := time.String()
	return &str
}
