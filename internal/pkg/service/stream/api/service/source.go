package service

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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

func (s *service) DeleteSource(ctx context.Context, d dependencies.SourceRequestScope, payload *api.DeleteSourcePayload) (*api.Task, error) {
	// If user is not admin deny access for write
	token := d.StorageAPIToken()
	if token.Admin == nil || token.Admin.Role != adminRole {
		return nil, svcerrors.NewForbiddenError(s.adminError)
	}

	// Quick check before the task
	if err := s.sourceMustExist(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

	cascade := payload.Cascade

	// Delete source in a task
	t, err := s.startTask(ctx, taskConfig{
		Type:      "delete.source",
		Timeout:   5 * time.Minute,
		ProjectID: d.ProjectID(),
		ObjectKey: d.SourceKey(),
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			start := time.Now()

			var err error
			var source definition.Source

			// In cascade mode, serialize against concurrent sink create/update on this source
			// (sink.go uses the same lock key) and capture every sink the source owns - active
			// AND already-soft-deleted - so their destination tables are included in cleanup.
			var ownedSinks []definition.Sink
			if cascade {
				var unlock func()
				unlock, ownedSinks, err = s.lockAndListAllSinks(ctx, d.SourceKey())
				if unlock != nil {
					defer unlock()
				}
			}

			if err == nil {
				source, err = s.definition.Source().SoftDelete(d.SourceKey(), s.clock.Now(), d.RequestUser()).Do(ctx).ResultOrErr()
			}

			formatMsg := func(err error) string {
				if err != nil {
					return "Source delete failed."
				}

				return "Source delete done."
			}

			defer func() {
				// Use the request-scope key for telemetry: if any pre-SoftDelete step fails, the
				// captured "source" is zero and source.SourceKey.String() would panic via
				// SourceID.String. d.SourceKey() is always populated from the request.
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
						SourceID:   d.SourceKey().SourceID,
						SourceKey:  d.SourceKey(),
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

			if !cascade {
				result := task.OkResult("Source has been deleted successfully.")
				return s.mapper.WithTaskOutputs(result, d.SourceKey())
			}

			// Hard-delete the definitions so the same name can be reused immediately.
			if err = s.purgeSourceDefinitions(ctx, d.SourceKey()); err != nil {
				return task.ErrResult(err)
			}

			// Delete the destination Keboola tables and bucket. Failures here are not fatal -
			// the source is already gone, so report partial success instead of a generic error.
			result := task.OkResult("Source and its destination tables and bucket have been deleted.")
			result = s.mapper.WithTaskOutputs(result, d.SourceKey())
			if cleanupErr := s.cascadeDeleteKeboolaResources(ctx, d.KeboolaProjectAPI(), ownedSinks); cleanupErr != nil {
				logger.Warnf(ctx, "cascade cleanup incomplete: %v", cleanupErr)
				// The source is already deleted; keep the task outputs so clients retain its reference.
				errResult := task.ErrResult(errors.Errorf("source deleted, but some destination resources could not be removed: %w", cleanupErr))
				return s.mapper.WithTaskOutputs(errResult, d.SourceKey())
			}

			return result
		},
	})
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

// lockAndListAllSinks acquires the same distlock that sink create/update uses (see sink.go) and
// returns every sink belonging to the source, from both the active and the deleted prefix. The
// returned unlock function (non-nil iff the lock was acquired) MUST be deferred by the caller so
// it releases the lock when the surrounding operation ends. The two prefixes never share a
// SinkKey, so a plain concatenation is sufficient (no dedup).
func (s *service) lockAndListAllSinks(ctx context.Context, sourceKey key.SourceKey) (unlock func(), sinks []definition.Sink, err error) {
	lock := s.locks.NewMutex(fmt.Sprintf("api.source.sinks.%s", sourceKey))
	if err = lock.Lock(ctx); err != nil {
		return nil, nil, err
	}
	unlock = func() {
		if uErr := lock.Unlock(ctx); uErr != nil {
			s.logger.Warnf(ctx, "cannot unlock lock %q: %s", lock.Key(), uErr)
		}
	}

	active, err := s.definition.Sink().List(sourceKey).Do(ctx).All()
	if err != nil {
		return unlock, nil, err
	}
	deleted, err := s.definition.Sink().ListDeleted(sourceKey).Do(ctx).All()
	if err != nil {
		return unlock, nil, err
	}

	sinks = append(sinks, active...)
	sinks = append(sinks, deleted...)
	return unlock, sinks, nil
}

// purgeSourceDefinitions hard-deletes the source and all its sinks from etcd (after a preceding
// SoftDelete), so the same key can be recreated fresh instead of being revived by Create.
func (s *service) purgeSourceDefinitions(ctx context.Context, k key.SourceKey) error {
	if err := s.definition.Sink().PurgeAllFrom(k).Do(ctx).Err(); err != nil {
		return err
	}
	return s.definition.Source().Purge(k).Do(ctx).Err()
}

// cascadeDeleteKeboolaResources force-deletes the destination Keboola tables of the given sinks and
// their (deduplicated) buckets. Already-missing resources are ignored; remaining failures are
// collected so the caller can report what could not be removed.
func (s *service) cascadeDeleteKeboolaResources(ctx context.Context, api *keboola.AuthorizedAPI, sinks []definition.Sink) error {
	tableKeys, bucketKeys := collectKeboolaTableResources(sinks)

	errs := errors.NewMultiError()
	for _, tableKey := range tableKeys {
		if err := api.DeleteTableRequest(tableKey, keboola.WithForce()).SendOrErr(ctx); err != nil && !isResourceNotFound(err) {
			errs.Append(errors.Errorf(`cannot delete table "%s": %w`, tableKey.TableID, err))
		}
	}
	for _, bucketKey := range bucketKeys {
		if err := api.DeleteBucketRequest(bucketKey, keboola.WithForce()).SendOrErr(ctx); err != nil && !isResourceNotFound(err) {
			errs.Append(errors.Errorf(`cannot delete bucket "%s": %w`, bucketKey.BucketID, err))
		}
	}
	return errs.ErrorOrNil()
}

// collectKeboolaTableResources returns the destination tables of the Keboola table sinks and their
// deduplicated buckets, preserving sink order (OTLP sources own several tables in a single bucket).
func collectKeboolaTableResources(sinks []definition.Sink) (tableKeys []keboola.TableKey, bucketKeys []keboola.BucketKey) {
	seenBuckets := make(map[keboola.BucketID]bool)
	for _, sink := range sinks {
		if sink.Type != definition.SinkTypeTable || sink.Table == nil || sink.Table.Type != definition.TableTypeKeboola || sink.Table.Keboola == nil {
			continue
		}
		tableKey := keboola.TableKey{BranchID: sink.BranchID, TableID: sink.Table.Keboola.TableID}
		tableKeys = append(tableKeys, tableKey)
		bucketKey := tableKey.BucketKey()
		if !seenBuckets[bucketKey.BucketID] {
			seenBuckets[bucketKey.BucketID] = true
			bucketKeys = append(bucketKeys, bucketKey)
		}
	}
	return tableKeys, bucketKeys
}

// isResourceNotFound reports whether the error is a Storage API "not found" error for a table or
// bucket, in which case cascade deletion can treat it as already done.
func isResourceNotFound(err error) bool {
	var apiErr *keboola.StorageError
	return errors.As(err, &apiErr) && (apiErr.ErrCode == "storage.tables.notFound" || apiErr.ErrCode == "storage.buckets.notFound")
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

func (s *service) TestSource(ctx context.Context, d dependencies.SourceRequestScope, payload *api.TestSourcePayload, _ io.ReadCloser) (res *api.TestResult, err error) {
	source, err := s.definition.Source().Get(d.SourceKey()).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}

	sinks, err := s.definition.Sink().List(d.SourceKey()).Do(ctx).All()
	if err != nil {
		return nil, err
	}

	// Remove X-StorageApi-Token from headers
	req := d.Request()
	req.Header.Del("x-storageapi-token")

	var recordCtx recordctx.Context
	if source.Type == definition.SourceTypeOTLP {
		// For OTLP sources the test body is interpreted as an already-flattened
		// OTLP record (the same structure that FlattenLogs/Metrics/Traces produces).
		// This lets API callers test their jsonnet column expressions against a
		// realistic flat payload without having to send a real protobuf batch.
		// The signal selector is validated by the Goa enum; default to "logs"
		// when the query parameter is omitted.
		signal := "logs"
		if payload != nil && payload.Signal != nil {
			signal = string(*payload.Signal)
		}
		recordCtx, err = recordctx.FromOTLPTestRequest(ctx, d.Clock().Now(), req, signal)
		if err != nil {
			return nil, svcerrors.NewBadRequestError(err)
		}
		// Sinks whose AllowedSignals filter rejects this signal would be skipped
		// during real ingestion; rendering them here would surface mapping errors
		// from sinks that never see this record at runtime.
		sinks = filterSinksBySignal(sinks, recordCtx.Signal())
	} else {
		recordCtx = recordctx.FromHTTP(d.Clock().Now(), req)
	}

	return s.mapper.NewTestResultResponse(d.SourceKey(), sinks, recordCtx)
}

// filterSinksBySignal keeps only sinks whose AcceptsSignal returns true.
// Delegates to definition.SignalAccepted so the /test endpoint stays in lock
// step with the runtime router's per-signal dispatch decision.
func filterSinksBySignal(sinks []definition.Sink, signal string) []definition.Sink {
	out := make([]definition.Sink, 0, len(sinks))
	for _, sink := range sinks {
		if sink.AcceptsSignal(signal) {
			out = append(out, sink)
		}
	}
	return out
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

	result := d.StatisticsRepository().ResetAllSinksStats(sinkKeys).Do(ctx)
	s.logger.Infof(ctx, `Statistics clear for source "%s" used %d operations`, d.SourceKey().String(), result.MaxOps())

	return result.Err()
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

func (s *service) RotateSourceSecret(ctx context.Context, d dependencies.SourceRequestScope, _ *api.RotateSourceSecretPayload) (*api.Source, error) {
	// If user is not admin deny access for write
	token := d.StorageAPIToken()
	if token.Admin == nil || token.Admin.Role != adminRole {
		return nil, svcerrors.NewForbiddenError(s.adminError)
	}

	if err := s.sourceMustExist(ctx, d.SourceKey()); err != nil {
		return nil, err
	}

	// Rotating the secret is a single atomic update (no provisioning), so it runs synchronously and
	// returns the updated source with the refreshed URL. The ingest dispatcher mirrors source secrets
	// from etcd, so the new secret takes effect - and the old one stops working - once this commits.
	source, err := s.definition.Source().
		Update(d.SourceKey(), d.Clock().Now(), d.RequestUser(), "Rotated secret.", rotateSourceSecret).
		Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}

	return s.mapper.NewSourceResponse(source)
}

// rotateSourceSecret regenerates the 48-character secret of an HTTP or OTLP source, leaving the rest
// of the source unchanged.
func rotateSourceSecret(source definition.Source) (definition.Source, error) {
	switch source.Type {
	case definition.SourceTypeHTTP:
		source.HTTP.Secret = idgenerator.StreamHTTPSourceSecret()
	case definition.SourceTypeOTLP:
		source.OTLP.Secret = idgenerator.StreamHTTPSourceSecret()
	default:
		return definition.Source{}, svcerrors.NewBadRequestError(errors.Errorf(`cannot rotate secret of source type "%s"`, source.Type))
	}
	return source, nil
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
