package service

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"golang.org/x/exp/maps"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
			// Lock: create/modify only one sink per source at a time
			lock := s.locks.NewMutex(fmt.Sprintf("api.source.sinks.%s", sink.SourceKey))
			if err := lock.Lock(ctx); err != nil {
				return task.ErrResult(err)
			}
			defer func() {
				if err := lock.Unlock(ctx); err != nil {
					s.logger.Warnf(ctx, "cannot unlock lock %q: %s", lock.Key(), err)
				}
			}()

			// Create sink
			op := s.definition.Sink().Create(&sink, s.clock.Now(), d.RequestUser(), "New sink.")
			op = op.RequireLock(lock)
			if err := op.Do(ctx).Err(); err == nil {
				result := task.OkResult("Sink has been created successfully.")
				result = s.mapper.WithTaskOutputs(result, sink.SinkKey)
				return result
			} else {
				return task.ErrResult(err)
			}
		},
	})
	if err != nil {
		if errors.As(err, &task.TaskLockError{}) {
			return nil, svcerrors.NewResourceAlreadyExistsError("sink", sink.SinkKey.String(), "source")
		}

		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) GetSink(ctx context.Context, d dependencies.SinkRequestScope, _ *api.GetSinkPayload) (*api.Sink, error) {
	if err := s.sinkMustExist(ctx, d.SinkKey()); err != nil {
		return nil, err
	}

	sink, err := s.definition.Sink().Get(d.SinkKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}

	return s.mapper.NewSinkResponse(sink)
}

func (s *service) ListSinks(ctx context.Context, d dependencies.SourceRequestScope, payload *api.ListSinksPayload) (*api.SinksList, error) {
	if err := s.sourceMustExist(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

	list := func(opts ...iterator.Option) iterator.DefinitionT[definition.Sink] {
		return s.definition.Sink().List(d.SourceKey(), opts...)
	}

	return s.mapper.NewSinksResponse(ctx, d.SourceKey(), payload.AfterID, payload.Limit, list)
}

func (s *service) ListDeletedSinks(ctx context.Context, scope dependencies.SourceRequestScope, payload *api.ListDeletedSinksPayload) (res *api.SinksList, err error) {
	if err := s.sourceMustExist(ctx, scope.SourceKey()); err != nil {
		return nil, err
	}

	list := func(opts ...iterator.Option) iterator.DefinitionT[definition.Sink] {
		return s.definition.Sink().ListDeleted(scope.SourceKey(), opts...)
	}

	return s.mapper.NewSinksResponse(ctx, scope.SourceKey(), payload.AfterID, payload.Limit, list)
}

func (s *service) UpdateSink(ctx context.Context, d dependencies.SinkRequestScope, payload *api.UpdateSinkPayload) (*api.Task, error) {
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

	// Quick validation - without save and associated slow operations
	sink, err := s.definition.Sink().Get(d.SinkKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}
	if _, err := update(sink); err != nil {
		return nil, err
	}

	// Update sink in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "update.sink",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SinkKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			// Lock: create/modify only one sink per source at a time
			lock := s.locks.NewMutex(fmt.Sprintf("api.source.sinks.%s", sink.SourceKey))
			if err := lock.Lock(ctx); err != nil {
				return task.ErrResult(err)
			}
			defer func() {
				if err := lock.Unlock(ctx); err != nil {
					s.logger.Warnf(ctx, "cannot unlock lock %q: %s", lock.Key(), err)
				}
			}()

			// Update the sink, with retries on a collision
			if err := s.definition.Sink().Update(d.SinkKey(), s.clock.Now(), d.RequestUser(), changeDesc, update).Do(ctx).Err(); err == nil {
				result := task.OkResult("Sink has been updated successfully.")
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

func (s *service) DeleteSink(ctx context.Context, d dependencies.SinkRequestScope, _ *api.DeleteSinkPayload) (*api.Task, error) {
	// Quick check before the task
	if err := s.sinkMustExist(ctx, d.SinkKey()); err != nil {
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
	if err := s.sinkMustExist(ctx, d.SinkKey()); err != nil {
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
		ObjectKey: d.SinkKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			// Update the sink, with retries on a collision
			if err := s.definition.Sink().Update(d.SinkKey(), s.clock.Now(), d.RequestUser(), changeDesc, update).Do(ctx).Err(); err == nil {
				result := task.OkResult("Sink settings have been updated successfully.")
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

func (s *service) SinkStatisticsTotal(ctx context.Context, d dependencies.SinkRequestScope, _ *api.SinkStatisticsTotalPayload) (*api.SinkStatisticsTotalResult, error) {
	if err := s.sinkMustExist(ctx, d.SinkKey()); err != nil {
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

	lastReset, err := d.StatisticsRepository().LastReset(d.SinkKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}

	err = d.StorageRepository().File().ListRecentIn(d.SinkKey()).
		WithFilter(func(v model.File) bool {
			if lastReset.ResetAt == nil {
				return true
			}
			// Exclude files newer than last reset.
			return v.OpenedAt().After(*lastReset.ResetAt)
		}).
		Do(ctx).
		ForEachValue(func(value model.File, header *iterator.Header) error {
			filesMap[value.FileID] = s.mapper.NewSinkFile(value)
			return nil
		})
	if err != nil {
		return nil, err
	}

	if len(filesMap) > 0 {
		// Sort keys lexicographically
		keys := maps.Keys(filesMap)
		slices.SortStableFunc(keys, func(a, b model.FileID) int {
			return strings.Compare(a.String(), b.String())
		})

		statisticsMap, err := d.StatisticsRepository().FilesStats(d.SinkKey(), keys[0], keys[len(keys)-1]).Do(ctx).ResultOrErr()
		if err != nil {
			return nil, err
		}

		for key, aggregated := range statisticsMap {
			filesMap[key].Statistics = s.mapper.NewSinkFileStatistics(aggregated)
		}
	}

	// Sort files response by OpenedAt timestamp
	files := maps.Values(filesMap)
	slices.SortStableFunc(files, func(a, b *api.SinkFile) int {
		return strings.Compare(a.OpenedAt, b.OpenedAt)
	})

	return &stream.SinkStatisticsFilesResult{Files: files}, nil
}

func (s *service) SinkStatisticsClear(ctx context.Context, d dependencies.SinkRequestScope, payload *api.SinkStatisticsClearPayload) (err error) {
	if err := s.sinkMustExist(ctx, d.SinkKey()); err != nil {
		return err
	}

	return d.StatisticsRepository().ResetSinkStats(d.SinkKey()).Do(ctx).Err()
}

func (s *service) DisableSink(ctx context.Context, d dependencies.SinkRequestScope, payload *api.DisableSinkPayload) (res *api.Task, err error) {
	if err := s.sinkMustExist(ctx, d.SinkKey()); err != nil {
		return nil, err
	}

	// Disable sink in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "disable.sink",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SinkKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			if err := s.definition.Sink().Disable(d.SinkKey(), d.Clock().Now(), d.RequestUser(), "API").Do(ctx).Err(); err == nil {
				result := task.OkResult("Sink has been disabled successfully.")
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

func (s *service) EnableSink(ctx context.Context, d dependencies.SinkRequestScope, payload *api.EnableSinkPayload) (res *api.Task, err error) {
	if err := s.sinkMustExist(ctx, d.SinkKey()); err != nil {
		return nil, err
	}

	// Enable sink in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "enable.sink",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SinkKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			if err := s.definition.Sink().Enable(d.SinkKey(), d.Clock().Now(), d.RequestUser()).Do(ctx).Err(); err == nil {
				result := task.OkResult("Sink has been enabled successfully.")
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

func (s *service) UndeleteSink(ctx context.Context, scope dependencies.SinkRequestScope, payload *api.UndeleteSinkPayload) (res *api.Task, err error) {
	if err := s.sourceMustExist(ctx, scope.SourceKey()); err != nil {
		return nil, err
	}

	if err := s.sinkMustBeDeleted(ctx, scope.SinkKey()); err != nil {
		return nil, err
	}

	t, err := s.startTask(ctx, taskConfig{
		Type:      "undelete.sink",
		Timeout:   5 * time.Minute,
		ProjectID: scope.ProjectID(),
		ObjectKey: scope.SinkKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			if err := s.definition.Sink().Undelete(scope.SinkKey(), scope.Clock().Now(), scope.RequestUser()).Do(ctx).Err(); err != nil {
				return task.ErrResult(err)
			}
			result := task.OkResult("Sink has been undeleted successfully.")
			result = s.mapper.WithTaskOutputs(result, scope.SinkKey())
			return result
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) ListSinkVersions(ctx context.Context, scope dependencies.SinkRequestScope, payload *api.ListSinkVersionsPayload) (res *api.EntityVersions, err error) {
	if err := s.sinkMustExist(ctx, scope.SinkKey()); err != nil {
		return nil, err
	}

	list := func(opts ...iterator.Option) iterator.DefinitionT[definition.Sink] {
		opts = append(opts,
			iterator.WithLimit(payload.Limit),
			iterator.WithStartOffset(formatAfterID(payload.AfterID), false),
		)
		return s.definition.Sink().ListVersions(scope.SinkKey(), opts...)
	}

	return s.mapper.NewSinkVersions(ctx, formatAfterID(payload.AfterID), payload.Limit, list)
}

func (s *service) SinkVersionDetail(ctx context.Context, scope dependencies.SinkRequestScope, payload *api.SinkVersionDetailPayload) (res *api.Version, err error) {
	if err := s.sinkMustExist(ctx, scope.SinkKey()); err != nil {
		return nil, err
	}

	sink, err := s.definition.Sink().Version(scope.SinkKey(), payload.VersionNumber).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}

	return s.mapper.NewVersionResponse(sink.Version), nil
}

func (s *service) RollbackSinkVersion(ctx context.Context, scope dependencies.SinkRequestScope, payload *api.RollbackSinkVersionPayload) (res *api.Task, err error) {
	if err := s.sinkVersionMustExist(ctx, scope.SinkKey(), payload.VersionNumber); err != nil {
		return nil, err
	}

	t, err := s.startTask(ctx, taskConfig{
		Type:      "rollback.sinkVersion",
		Timeout:   5 * time.Minute,
		ProjectID: scope.ProjectID(),
		ObjectKey: scope.SinkKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			if err = s.definition.Sink().RollbackVersion(scope.SinkKey(), s.clock.Now(), scope.RequestUser(), payload.VersionNumber).Do(ctx).Err(); err != nil {
				return task.ErrResult(err)
			}

			result := task.OkResult("Sink version was rolled back successfully.")
			result = s.mapper.WithTaskOutputs(result, scope.SinkKey())
			return result
		},
	})

	return s.mapper.NewTaskResponse(t)
}

func (s *service) sinkMustNotExist(ctx context.Context, k key.SinkKey) error {
	return s.definition.Sink().MustNotExist(k).Do(ctx).Err()
}

func (s *service) sinkMustExist(ctx context.Context, k key.SinkKey) error {
	return s.definition.Sink().ExistsOrErr(k).Do(ctx).Err()
}

func (s *service) sinkVersionMustExist(ctx context.Context, k key.SinkKey, number definition.VersionNumber) error {
	if err := s.sinkMustExist(ctx, k); err != nil {
		return err
	}

	return s.definition.Sink().Version(k, number).Do(ctx).Err()
}

func (s *service) sinkMustBeDeleted(ctx context.Context, k key.SinkKey) error {
	return s.definition.Sink().GetDeleted(k).Do(ctx).Err()
}
