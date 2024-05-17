package service

import (
	"context"
	"time"

	"golang.org/x/exp/maps"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
)

//nolint:dupl // CreateSource method is similar
func (s *service) CreateSink(ctx context.Context, d dependencies.SourceRequestScope, payload *api.CreateSinkPayload) (*api.Task, error) {
	// Create entity
	sink, err := s.mapper.NewSinkEntity(d.SourceKey(), payload)
	if err != nil {
		return nil, err
	}

	// Quick check before the task
	if err := s.sinkMustNotExist(ctx, sink.SinkKey); err != nil {
		return nil, err
	}

	// Create sink in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "create.sink",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: sink.SinkKey,
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			if err := s.definition.Sink().Create(&sink, s.clock.Now(), d.RequestUser(), "New sink.").Do(ctx).Err(); err == nil {
				result := task.OkResult("Sink has been created successfully.")
				result = s.mapper.WithTaskOutputs(result, sink.SinkKey)
				return result
			} else {
				return task.ErrResult(err)
			}
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) GetSink(ctx context.Context, d dependencies.SinkRequestScope, _ *api.GetSinkPayload) (*api.Sink, error) {
	if err := s.sinkMustExists(ctx, d.SinkKey()); err != nil {
		return nil, err
	}

	sink, err := s.definition.Sink().Get(d.SinkKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}

	return s.mapper.NewSinkResponse(sink)
}

func (s *service) ListSinks(ctx context.Context, d dependencies.SourceRequestScope, payload *api.ListSinksPayload) (*api.SinksList, error) {
	if err := s.sourceMustExists(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

	list := func(opts ...iterator.Option) iterator.DefinitionT[definition.Sink] {
		return s.definition.Sink().List(d.SourceKey(), opts...)
	}

	return s.mapper.NewSinksResponse(ctx, d.SourceKey(), payload.SinceID, payload.Limit, list)
}

func (s *service) UpdateSink(ctx context.Context, d dependencies.SinkRequestScope, payload *api.UpdateSinkPayload) (*api.Task, error) {
	// Quick check before the task
	if err := s.sinkMustExists(ctx, d.SinkKey()); err != nil {
		return nil, err
	}

	// Get the change description
	var changeDesc string
	if payload.ChangeDescription == nil {
		changeDesc = "Updated."
	} else {
		changeDesc = *payload.ChangeDescription
	}

	// Define update function
	update := func(sink definition.Sink) (definition.Sink, error) {
		return s.mapper.UpdateSinkEntity(sink, payload)
	}

	// Update sink in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "update.sink",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			// Update the source, with retries on a collision
			if err := s.definition.Sink().Update(d.SinkKey(), s.clock.Now(), d.RequestUser(), changeDesc, update).Do(ctx).Err(); err == nil {
				result := task.OkResult("Sink has been updated successfully.")
				result = s.mapper.WithTaskOutputs(result, d.SourceKey())
				return result
			} else {
				return task.ErrResult(err)
			}
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) DeleteSink(ctx context.Context, d dependencies.SinkRequestScope, _ *api.DeleteSinkPayload) (*api.Task, error) {
	// Quick check before the task
	if err := s.sinkMustExists(ctx, d.SinkKey()); err != nil {
		return nil, err
	}

	// Delete sink in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "delete.sink",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SinkKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			if err := s.definition.Sink().SoftDelete(d.SinkKey(), s.clock.Now(), d.RequestUser()).Do(ctx).Err(); err == nil {
				result := task.OkResult("Sink has been deleted successfully.")
				result = s.mapper.WithTaskOutputs(result, d.SinkKey())
				return result
			} else {
				return task.ErrResult(err)
			}
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) GetSinkSettings(ctx context.Context, d dependencies.SinkRequestScope, _ *api.GetSinkSettingsPayload) (*api.SettingsResult, error) {
	source, err := s.definition.Sink().Get(d.SinkKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}
	return s.mapper.NewSettingsResponse(source.Config)
}

func (s *service) UpdateSinkSettings(ctx context.Context, d dependencies.SinkRequestScope, payload *api.UpdateSinkSettingsPayload) (*api.Task, error) {
	// Quick check before the task
	if err := s.sinkMustExists(ctx, d.SinkKey()); err != nil {
		return nil, err
	}

	// Get the change description
	var changeDesc string
	if payload.ChangeDescription == nil {
		changeDesc = "Updated settings."
	} else {
		changeDesc = *payload.ChangeDescription
	}

	// Prepare patch KVs
	patch, err := s.mapper.NewSettingsPatch(payload.Settings, false)
	if err != nil {
		return nil, err
	}

	// Define update function
	update := func(sink definition.Sink) (definition.Sink, error) {
		sink.Config = sink.Config.With(patch)
		return sink, err
	}

	// Update sink settings in a task, it triggers file rotation
	t, err := s.startTask(ctx, taskConfig{
		Type:      "update.sinkSettings",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			// Update the source, with retries on a collision
			if err := s.definition.Sink().Update(d.SinkKey(), s.clock.Now(), d.RequestUser(), changeDesc, update).Do(ctx).Err(); err == nil {
				result := task.OkResult("Sink settings have been updated successfully.")
				result = s.mapper.WithTaskOutputs(result, d.SourceKey())
				return result
			} else {
				return task.ErrResult(err)
			}
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) SinkStatisticsTotal(ctx context.Context, d dependencies.SinkRequestScope, _ *api.SinkStatisticsTotalPayload) (*api.SinkStatisticsTotalResult, error) {
	if err := s.sinkMustExists(ctx, d.SinkKey()); err != nil {
		return nil, err
	}

	stats, err := d.StatisticsRepository().SinkStats(ctx, d.SinkKey())
	if err != nil {
		return nil, err
	}

	return s.mapper.NewSinkStatisticsTotalResponse(stats), nil
}

func (s *service) SinkStatisticsFiles(ctx context.Context, d dependencies.SinkRequestScope, payload *stream.SinkStatisticsFilesPayload) (res *stream.SinkStatisticsFilesResult, err error) {
	err = s.definition.Sink().ExistsOrErr(d.SinkKey()).Do(ctx).Err()
	if err != nil {
		return nil, err
	}

	filesMap := make(map[model.FileID]*stream.SinkFile)

	err = d.StorageRepository().File().ListRecentIn(d.SinkKey()).Do(ctx).ForEachValue(func(value model.File, header *iterator.Header) error {
		out := &stream.SinkFile{
			State:       value.State,
			OpenedAt:    value.OpenedAt().String(),
			ClosingAt:   timeToString(value.ClosingAt),
			ImportingAt: timeToString(value.ImportingAt),
			ImportedAt:  timeToString(value.ImportedAt),
		}

		if value.RetryAttempt > 0 {
			out.RetryAttempt = ptr.Ptr(value.RetryAttempt)
			out.RetryReason = ptr.Ptr(value.RetryReason)
			out.RetryAfter = ptr.Ptr(value.RetryAfter.String())
		}

		filesMap[value.FileID] = out
		return nil
	})
	if err != nil {
		return nil, err
	}

	keys := maps.Keys(filesMap)
	if len(keys) > 0 {
		statisticsMap, err := d.StatisticsRepository().FilesStats(d.SinkKey(), keys[0], keys[len(keys)-1]).Do(ctx).ResultOrErr()
		if err != nil {
			return nil, err
		}

		for key, aggregated := range statisticsMap {
			if filesMap[key].Statistics == nil {
				filesMap[key].Statistics = &stream.SinkFileStatistics{
					Levels: &stream.Levels{
						Local:   &stream.Level{},
						Staging: &stream.Level{},
						Target:  &stream.Level{},
					},
					Total: &stream.Level{},
				}
			}
			assignStatistics(filesMap[key].Statistics.Total, aggregated.Total)
			assignStatistics(filesMap[key].Statistics.Levels.Local, aggregated.Local)
			assignStatistics(filesMap[key].Statistics.Levels.Staging, aggregated.Staging)
			assignStatistics(filesMap[key].Statistics.Levels.Target, aggregated.Target)
		}

		for _, file := range filesMap {
			if file.Statistics.Total.RecordsCount == 0 {
				file.Statistics.Total = nil
			}
			if file.Statistics.Levels.Local.RecordsCount == 0 {
				file.Statistics.Levels.Local = nil
			}
			if file.Statistics.Levels.Staging.RecordsCount == 0 {
				file.Statistics.Levels.Staging = nil
			}
			if file.Statistics.Levels.Target.RecordsCount == 0 {
				file.Statistics.Levels.Target = nil
			}
		}
	}

	res = &stream.SinkStatisticsFilesResult{
		Files: maps.Values(filesMap),
	}

	return res, nil
}

func (s *service) sinkMustNotExist(ctx context.Context, k key.SinkKey) error {
	return s.definition.Sink().MustNotExists(k).Do(ctx).Err()
}

func (s *service) sinkMustExists(ctx context.Context, k key.SinkKey) error {
	return s.definition.Sink().ExistsOrErr(k).Do(ctx).Err()
}

func assignStatistics(levelStatistics *stream.Level, levelValue statistics.Value) {
	levelStatistics.FirstRecordAt = timeToString(&levelValue.FirstRecordAt)
	levelStatistics.LastRecordAt = timeToString(&levelValue.LastRecordAt)
	levelStatistics.RecordsCount = levelValue.RecordsCount
	levelStatistics.UncompressedSize = uint64(levelValue.UncompressedSize)
}

func timeToString(time *utctime.UTCTime) *string {
	if time == nil || time.IsZero() {
		return nil
	}

	return ptr.Ptr(time.String())
}
