package service

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge"
)

//nolint:dupl // CreateSink method is similar
func (s *service) CreateSource(ctx context.Context, d dependencies.BranchRequestScope, payload *api.CreateSourcePayload) (*api.Task, error) {
	// If user is not admin deny access for write
	token := d.StorageAPIToken()
	if token.Admin == nil || token.Admin.Role != adminRole {
		return nil, svcerrors.NewForbiddenError(s.adminError)
	}

	// Create entity
	source, err := s.mapper.NewSourceEntity(d.BranchKey(), payload)
	if err != nil {
		return nil, err
	}

	// Quick check before the task
	if err := s.sourceMustNotExist(ctx, source.SourceKey); err != nil {
		return nil, err
	}

	// Create source in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "create.source",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: source.SourceKey,
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			start := time.Now()
			err := s.definition.Source().Create(&source, s.clock.Now(), d.RequestUser(), "New source.").Do(ctx).Err()
			formatMsg := func(err error) string {
				if err != nil {
					return "Source create failed."
				}

				return "Source create done."
			}

			defer func() {
				sErr := bridge.SendEvent(
					ctx,
					logger,
					d.KeboolaProjectAPI(),
					bridge.ComponentSourceCreateID,
					time.Since(start),
					err,
					formatMsg,
					bridge.Params{
						ProjectID:  d.ProjectID(),
						BranchID:   d.Branch().BranchID,
						SourceID:   source.SourceID,
						SourceKey:  source.SourceKey,
						SourceName: source.Name,
					},
				)
				if sErr != nil {
					logger.Warnf(ctx, "%v", sErr)
				}
			}()

			if err != nil {
				return task.ErrResult(err)
			}

			result := task.OkResult("Source has been created successfully.")
			result = s.mapper.WithTaskOutputs(result, source.SourceKey)
			return result
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) UpdateSource(ctx context.Context, d dependencies.SourceRequestScope, payload *api.UpdateSourcePayload) (*api.Task, error) {
	// If user is not admin deny access for write
	token := d.StorageAPIToken()
	if token.Admin == nil || token.Admin.Role != adminRole {
		return nil, svcerrors.NewForbiddenError(s.adminError)
	}

	// Get the change description
	var changeDesc string
	if payload.ChangeDescription == nil {
		changeDesc = "Updated."
	} else {
		changeDesc = *payload.ChangeDescription
	}

	// Define update function
	update := func(source definition.Source) (definition.Source, error) {
		return s.mapper.UpdateSourceEntity(source, payload)
	}

	// Quick validation - without save and associated slow operations
	source, err := s.definition.Source().Get(d.SourceKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}
	if _, err := update(source); err != nil {
		return nil, err
	}

	// Update source in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "update.source",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			// Update the source, with retries on a collision
			if err := s.definition.Source().Update(d.SourceKey(), s.clock.Now(), d.RequestUser(), changeDesc, update).Do(ctx).Err(); err != nil {
				return task.ErrResult(err)
			}

			result := task.OkResult("Source has been updated successfully.")
			result = s.mapper.WithTaskOutputs(result, d.SourceKey())
			return result
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) ListSources(ctx context.Context, d dependencies.BranchRequestScope, payload *api.ListSourcesPayload) (*api.SourcesList, error) {
	list := func(opts ...iterator.Option) iterator.DefinitionT[definition.Source] {
		return s.definition.Source().List(d.BranchKey(), opts...)
	}
	return s.mapper.NewSourcesResponse(ctx, d.BranchKey(), payload.AfterID, payload.Limit, list)
}

func (s *service) ListDeletedSources(ctx context.Context, scope dependencies.BranchRequestScope, payload *api.ListDeletedSourcesPayload) (res *api.SourcesList, err error) {
	list := func(opts ...iterator.Option) iterator.DefinitionT[definition.Source] {
		return s.definition.Source().ListDeleted(scope.BranchKey(), opts...)
	}
	return s.mapper.NewSourcesResponse(ctx, scope.BranchKey(), payload.AfterID, payload.Limit, list)
}

func (s *service) GetSource(ctx context.Context, d dependencies.SourceRequestScope, _ *api.GetSourcePayload) (*api.Source, error) {
	if err := s.sourceMustExist(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

	source, err := s.definition.Source().Get(d.SourceKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}

	return s.mapper.NewSourceResponse(source)
}

func (s *service) DeleteSource(ctx context.Context, d dependencies.SourceRequestScope, _ *api.DeleteSourcePayload) (*api.Task, error) {
	// If user is not admin deny access for write
	token := d.StorageAPIToken()
	if token.Admin == nil || token.Admin.Role != adminRole {
		return nil, svcerrors.NewForbiddenError(s.adminError)
	}

	// Quick check before the task
	if err := s.sourceMustExist(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

	// Delete source in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "delete.source",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			start := time.Now()
			source, err := s.definition.Source().SoftDelete(d.SourceKey(), s.clock.Now(), d.RequestUser()).Do(ctx).ResultOrErr()
			formatMsg := func(err error) string {
				if err != nil {
					return "Source delete failed."
				}

				return "Source delete done."
			}

			defer func() {
				sErr := bridge.SendEvent(
					ctx,
					logger,
					d.KeboolaProjectAPI(),
					bridge.ComponentSourceDeleteID,
					time.Since(start),
					err,
					formatMsg,
					bridge.Params{
						ProjectID:  d.ProjectID(),
						BranchID:   d.Branch().BranchID,
						SourceID:   source.SourceID,
						SourceKey:  source.SourceKey,
						SourceName: source.Name,
					},
				)
				if sErr != nil {
					logger.Warnf(ctx, "%v", sErr)
				}
			}()

			if err != nil {
				return task.ErrResult(err)
			}

			result := task.OkResult("Source has been deleted successfully.")
			result = s.mapper.WithTaskOutputs(result, d.SourceKey())
			return result
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) GetSourceSettings(ctx context.Context, d dependencies.SourceRequestScope, _ *api.GetSourceSettingsPayload) (*api.SettingsResult, error) {
	source, err := s.definition.Source().Get(d.SourceKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}
	return s.mapper.NewSettingsResponse(source.Config)
}

func (s *service) UpdateSourceSettings(ctx context.Context, d dependencies.SourceRequestScope, payload *api.UpdateSourceSettingsPayload) (*api.Task, error) {
	// If user is not admin deny access for write
	token := d.StorageAPIToken()
	if token.Admin == nil || token.Admin.Role != adminRole {
		return nil, svcerrors.NewForbiddenError(s.adminError)
	}

	// Quick check before the task
	if err := s.sourceMustExist(ctx, d.SourceKey()); err != nil {
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
	update := func(source definition.Source) (definition.Source, error) {
		source.Config = source.Config.With(patch)
		return source, err
	}

	// Update source settings in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "update.sourceSettings",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			// Update the source, with retries on a collision
			if err := s.definition.Source().Update(d.SourceKey(), s.clock.Now(), d.RequestUser(), changeDesc, update).Do(ctx).Err(); err != nil {
				return task.ErrResult(err)
			}

			result := task.OkResult("Source settings have been updated successfully.")
			result = s.mapper.WithTaskOutputs(result, d.SourceKey())
			return result
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) TestSource(ctx context.Context, d dependencies.SourceRequestScope, _ *api.TestSourcePayload, _ io.ReadCloser) (res *api.TestResult, err error) {
	if err := s.sourceMustExist(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

	sinks, err := s.definition.Sink().List(d.SourceKey()).Do(ctx).All()
	if err != nil {
		return nil, err
	}

	// Remove X-StorageApi-Token from headers
	req := d.Request()
	req.Header.Del("x-storageapi-token")

	recordCtx := recordctx.FromHTTP(d.Clock().Now(), req)

	return s.mapper.NewTestResultResponse(d.SourceKey(), sinks, recordCtx)
}

func (s *service) SourceStatisticsClear(ctx context.Context, d dependencies.SourceRequestScope, payload *api.SourceStatisticsClearPayload) (err error) {
	// If user is not admin deny access for write
	token := d.StorageAPIToken()
	if token.Admin == nil || token.Admin.Role != adminRole {
		return svcerrors.NewForbiddenError(s.adminError)
	}

	if err := s.sourceMustExist(ctx, d.SourceKey()); err != nil {
		return err
	}

	sinks, err := s.definition.Sink().List(d.SourceKey()).Do(ctx).All()
	if err != nil {
		return err
	}

	sinkKeys := make([]key.SinkKey, 0, len(sinks))
	for _, sink := range sinks {
		sinkKeys = append(sinkKeys, sink.SinkKey)
	}

	return d.StatisticsRepository().ResetAllSinksStats(ctx, sinkKeys)
}

func (s *service) DisableSource(ctx context.Context, d dependencies.SourceRequestScope, payload *api.DisableSourcePayload) (*api.Task, error) {
	// If user is not admin deny access for write
	token := d.StorageAPIToken()
	if token.Admin == nil || token.Admin.Role != adminRole {
		return nil, svcerrors.NewForbiddenError(s.adminError)
	}

	if err := s.sourceMustExist(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

	// Disable source in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "disable.source",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			start := time.Now()
			source, err := s.definition.Source().Disable(d.SourceKey(), d.Clock().Now(), d.RequestUser(), "API").Do(ctx).ResultOrErr()
			formatMsg := func(err error) string {
				if err != nil {
					return "Source disable failed."
				}

				return "Source disable done."
			}

			defer func() {
				sErr := bridge.SendEvent(
					ctx,
					logger,
					d.KeboolaProjectAPI(),
					bridge.ComponentSourceDisableID,
					time.Since(start),
					err,
					formatMsg,
					bridge.Params{
						ProjectID:  d.ProjectID(),
						BranchID:   d.Branch().BranchID,
						SourceID:   source.SourceID,
						SourceKey:  source.SourceKey,
						SourceName: source.Name,
					},
				)
				if sErr != nil {
					logger.Warnf(ctx, "%v", sErr)
				}
			}()

			if err != nil {
				return task.ErrResult(err)
			}

			result := task.OkResult("Source has been disabled successfully.")
			result = s.mapper.WithTaskOutputs(result, d.SourceKey())
			return result
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) EnableSource(ctx context.Context, d dependencies.SourceRequestScope, payload *api.EnableSourcePayload) (*api.Task, error) {
	// If user is not admin deny access for write
	token := d.StorageAPIToken()
	if token.Admin == nil || token.Admin.Role != adminRole {
		return nil, svcerrors.NewForbiddenError(s.adminError)
	}

	if err := s.sourceMustExist(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

	// Enable source in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "enable.source",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			start := time.Now()
			source, err := s.definition.Source().Enable(d.SourceKey(), d.Clock().Now(), d.RequestUser()).Do(ctx).ResultOrErr()
			formatMsg := func(err error) string {
				if err != nil {
					return "Source enable failed."
				}

				return "Source enable done."
			}

			defer func() {
				sErr := bridge.SendEvent(
					ctx,
					logger,
					d.KeboolaProjectAPI(),
					bridge.ComponentSourceEnableID,
					time.Since(start),
					err,
					formatMsg,
					bridge.Params{
						ProjectID:  d.ProjectID(),
						BranchID:   d.Branch().BranchID,
						SourceID:   source.SourceID,
						SourceKey:  source.SourceKey,
						SourceName: source.Name,
					},
				)
				if sErr != nil {
					logger.Warnf(ctx, "%v", sErr)
				}
			}()

			if err != nil {
				return task.ErrResult(err)
			}

			result := task.OkResult("Source has been enabled successfully.")
			result = s.mapper.WithTaskOutputs(result, d.SourceKey())
			return result
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) UndeleteSource(ctx context.Context, scope dependencies.SourceRequestScope, payload *api.UndeleteSourcePayload) (*api.Task, error) {
	// If user is not admin deny access for write
	token := scope.StorageAPIToken()
	if token.Admin == nil || token.Admin.Role != adminRole {
		return nil, svcerrors.NewForbiddenError(s.adminError)
	}

	// Quick check before the task
	if err := s.sourceMustBeDeleted(ctx, scope.SourceKey()); err != nil {
		return nil, err
	}

	// Undelete source in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "undelete.source",
		Timeout:   5 * time.Minute,
		ProjectID: scope.ProjectID(),
		ObjectKey: scope.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			start := time.Now()
			source, err := s.definition.Source().Undelete(scope.SourceKey(), scope.Clock().Now(), scope.RequestUser()).Do(ctx).ResultOrErr()
			formatMsg := func(err error) string {
				if err != nil {
					return "Source undelete failed."
				}

				return "Source undelete done."
			}

			defer func() {
				sErr := bridge.SendEvent(
					ctx,
					logger,
					scope.KeboolaProjectAPI(),
					bridge.ComponentSourceUndeleteID,
					time.Since(start),
					err,
					formatMsg,
					bridge.Params{
						ProjectID:  scope.ProjectID(),
						BranchID:   scope.Branch().BranchID,
						SourceID:   source.SourceID,
						SourceKey:  source.SourceKey,
						SourceName: source.Name,
					},
				)
				if sErr != nil {
					logger.Warnf(ctx, "%v", sErr)
				}
			}()

			if err != nil {
				return task.ErrResult(err)
			}

			result := task.OkResult("Source has been undeleted successfully.")
			result = s.mapper.WithTaskOutputs(result, scope.SourceKey())
			return result
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) ListSourceVersions(ctx context.Context, scope dependencies.SourceRequestScope, payload *api.ListSourceVersionsPayload) (res *api.EntityVersions, err error) {
	if err := s.sourceMustExist(ctx, scope.SourceKey()); err != nil {
		return nil, err
	}

	list := func(opts ...iterator.Option) iterator.DefinitionT[definition.Source] {
		opts = append(opts,
			iterator.WithLimit(payload.Limit),
			iterator.WithStartOffset(formatAfterID(payload.AfterID), false),
		)
		return s.definition.Source().ListVersions(scope.SourceKey(), opts...)
	}

	return s.mapper.NewSourceVersions(ctx, payload.AfterID, payload.Limit, list)
}

func (s *service) SourceVersionDetail(ctx context.Context, scope dependencies.SourceRequestScope, payload *api.SourceVersionDetailPayload) (res *api.Version, err error) {
	if err := s.sourceMustExist(ctx, scope.SourceKey()); err != nil {
		return nil, err
	}

	source, err := s.definition.Source().Version(scope.SourceKey(), payload.VersionNumber).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}

	return s.mapper.NewVersionResponse(source.Version), nil
}

func (s *service) RollbackSourceVersion(ctx context.Context, scope dependencies.SourceRequestScope, payload *api.RollbackSourceVersionPayload) (res *api.Task, err error) {
	// If user is not admin deny access for write
	token := scope.StorageAPIToken()
	if token.Admin == nil || token.Admin.Role != adminRole {
		return nil, svcerrors.NewForbiddenError(s.adminError)
	}

	if err := s.sourceVersionMustExist(ctx, scope.SourceKey(), payload.VersionNumber); err != nil {
		return nil, err
	}

	t, err := s.startTask(ctx, taskConfig{
		Type:      "rollback.sourceVersion",
		Timeout:   5 * time.Minute,
		ProjectID: scope.ProjectID(),
		ObjectKey: scope.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			if err = s.definition.Source().RollbackVersion(scope.SourceKey(), s.clock.Now(), scope.RequestUser(), payload.VersionNumber).Do(ctx).Err(); err != nil {
				return task.ErrResult(err)
			}

			result := task.OkResult("Source version was rolled back successfully.")
			result = s.mapper.WithTaskOutputs(result, scope.SourceKey())
			return result
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) sourceMustNotExist(ctx context.Context, k key.SourceKey) error {
	return s.definition.Source().MustNotExist(k).Do(ctx).Err()
}

func (s *service) sourceMustExist(ctx context.Context, k key.SourceKey) error {
	return s.definition.Source().ExistsOrErr(k).Do(ctx).Err()
}

func (s *service) sourceVersionMustExist(ctx context.Context, k key.SourceKey, number definition.VersionNumber) error {
	if err := s.sourceMustExist(ctx, k); err != nil {
		return err
	}
	return s.definition.Source().Version(k, number).Do(ctx).Err()
}

func (s *service) sourceMustBeDeleted(ctx context.Context, k key.SourceKey) error {
	return s.definition.Source().GetDeleted(k).Do(ctx).Err()
}

// FormatAfterID pads the given id string with leading zeros to ensure it is 10 characters long.
// If the input id is an empty string, it returns the empty string without any modification.
func formatAfterID(id string) string {
	if id == "" {
		return id
	}
	return fmt.Sprintf("%010s", id)
}
