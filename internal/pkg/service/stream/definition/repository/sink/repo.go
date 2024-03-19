package sink

import (
	"context"
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/sink/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/source"
	plugin2 "github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

const (
	MaxSinksPerSource      = 100
	MaxSinkVersionsPerSink = 1000
)

type Repository struct {
	client  etcd.KV
	schema  schema.Sink
	plugins *plugin2.Plugins
	sources *source.Repository
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin2.Plugins
}

func NewRepository(d dependencies, sources *source.Repository) *Repository {
	r := &Repository{
		client:  d.EtcdClient(),
		schema:  schema.ForSink(d.EtcdSerde()),
		plugins: d.Plugins(),
		sources: sources,
	}

	// Delete/undelete source with branch
	r.plugins.Collection().OnSourceSave(func(ctx *plugin2.SaveContext, v *definition.Source) {
		if v.DeletedAt != nil && v.DeletedAt.Time().Equal(ctx.Now()) {
			ctx.AddAtomicOp(r.softDeleteAllFrom(v.SourceKey, ctx.Now(), true))
		} else if v.UndeletedAt != nil && v.UndeletedAt.Time().Equal(ctx.Now()) {
			ctx.AddAtomicOp(r.undeleteAllFrom(v.SourceKey, ctx.Now(), true))
		}
	})

	return r
}

func (r *Repository) List(parentKey any) iterator.DefinitionT[definition.Sink] {
	return r.list(r.schema.Active(), parentKey)
}

func (r *Repository) ListDeleted(parentKey any) iterator.DefinitionT[definition.Sink] {
	return r.list(r.schema.Deleted(), parentKey)
}

func (r *Repository) list(pfx schema.SinkInState, parentKey any) iterator.DefinitionT[definition.Sink] {
	return pfx.In(parentKey).GetAll(r.client)
}

func (r *Repository) ExistsOrErr(k key.SinkKey) op.WithResult[bool] {
	return r.schema.
		Active().ByKey(k).Exists(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("sink", k.SinkID.String(), "source")
		})
}

func (r *Repository) Get(k key.SinkKey) op.WithResult[definition.Sink] {
	return r.schema.
		Active().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("sink", k.SinkID.String(), "source")
		})
}

func (r *Repository) GetDeleted(k key.SinkKey) op.WithResult[definition.Sink] {
	return r.schema.
		Deleted().ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("deleted sink", k.SinkID.String(), "source")
		})
}

//nolint:dupl // similar code is in the SourceRepository
func (r *Repository) Create(input *definition.Sink, now time.Time, versionDescription string) *op.AtomicOp[definition.Sink] {
	k := input.SinkKey
	var entity definition.Sink
	var actual, deleted *op.KeyValueT[definition.Sink]

	atomicOp := op.Atomic(r.client, &entity).
		// Check prerequisites
		ReadOp(r.sources.ExistsOrErr(entity.SourceKey)).
		ReadOp(r.checkMaxSinksPerSource(entity.SourceKey, 1)).
		// Get gets actual version to check if the entity already exists
		ReadOp(r.schema.Active().ByKey(k).GetKV(r.client).WithResultTo(&actual)).
		// GetDelete gets deleted version to check if we have to do undelete
		ReadOp(r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)).
		// Entity must not exist
		BeforeWriteOrErr(func(context.Context) error {
			if actual != nil {
				return serviceError.NewResourceAlreadyExistsError("sink", k.SinkID.String(), "source")
			}
			return nil
		}).
		// Init entity
		BeforeWrite(func(context.Context) {
			entity = *input
		}).
		// Set version/state from the deleted value, if any
		BeforeWrite(func(context.Context) {
			if deleted != nil {
				entity.Version = deleted.Value.Version
				entity.SoftDeletable = deleted.Value.SoftDeletable
				entity.Undelete(now)
			}
		}).
		// Increment version
		BeforeWrite(func(context.Context) {
			entity.IncrementVersion(entity, now, versionDescription)
		}).
		// Update the input entity after successful operation
		OnResult(func(entity definition.Sink) {
			*input = entity
		})

	// Save
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		return r.save(ctx, now, []definition.Sink{entity})
	})

	return atomicOp
}

//nolint:dupl // similar code is in the SourceRepository
func (r *Repository) Update(k key.SinkKey, now time.Time, versionDescription string, updateFn func(definition.Sink) (definition.Sink, error)) *op.AtomicOp[definition.Sink] {
	var entity definition.Sink
	atomicOp := op.Atomic(r.client, &entity).
		// Check prerequisites
		ReadOp(r.checkMaxSinksVersionsPerSink(k, 1)).
		// Read the entity
		ReadOp(r.Get(k).WithResultTo(&entity)).
		// Update the entity
		BeforeWriteOrErr(func(context.Context) error {
			if updated, err := updateFn(entity); err == nil {
				updated.IncrementVersion(entity, now, versionDescription)
				entity = updated
				return nil
			} else {
				return err
			}
		})

	// Save
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		return r.save(ctx, now, []definition.Sink{entity})
	})

	return atomicOp
}

func (r *Repository) SoftDelete(k key.SinkKey, now time.Time) *op.AtomicOp[definition.Sink] {
	var entity definition.Sink
	return op.Atomic(r.client, &entity).
		AddFrom(r.
			softDeleteAllFrom(k, now, false).
			OnResult(func(r []definition.Sink) {
				if len(r) == 1 {
					entity = r[0]
				}
			}))
}

func (r *Repository) Undelete(k key.SinkKey, now time.Time) *op.AtomicOp[definition.Sink] {
	var entity definition.Sink
	return op.Atomic(r.client, &entity).
		// Check prerequisites
		ReadOp(r.sources.ExistsOrErr(k.SourceKey)).
		ReadOp(r.checkMaxSinksPerSource(k.SourceKey, 1)).
		AddFrom(r.
			undeleteAllFrom(k, now, false).
			OnResult(func(r []definition.Sink) {
				if len(r) == 1 {
					entity = r[0]
				}
			}))
}

// Versions fetches all versions records for the object.
// The method can be used also for deleted objects.
func (r *Repository) Versions(k key.SinkKey) iterator.DefinitionT[definition.Sink] {
	return r.schema.Versions().Of(k).GetAll(r.client)
}

// Version fetch entity version.
// The method can be used also for deleted objects.
func (r *Repository) Version(k key.SinkKey, version definition.VersionNumber) op.WithResult[definition.Sink] {
	return r.schema.
		Versions().Of(k).Version(version).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("sink version", k.SinkID.String()+"/"+version.String(), "source")
		})
}

//nolint:dupl // similar code is in the SourceRepository
func (r *Repository) Rollback(k key.SinkKey, now time.Time, to definition.VersionNumber) *op.AtomicOp[definition.Sink] {
	var entity definition.Sink
	var latestVersion, targetVersion *op.KeyValueT[definition.Sink]

	atomicOp := op.Atomic(r.client, &entity).
		// Get latest version to calculate next version number
		ReadOp(r.schema.Versions().Of(k).GetOne(r.client, etcd.WithSort(etcd.SortByKey, etcd.SortDescend)).WithResultTo(&latestVersion)).
		// Get target version
		ReadOp(r.schema.Versions().Of(k).Version(to).GetKV(r.client).WithResultTo(&targetVersion)).
		// Return the most significant error
		BeforeWriteOrErr(func(context.Context) error {
			if latestVersion == nil {
				return serviceError.NewResourceNotFoundError("sink", k.SinkID.String(), "source")
			} else if targetVersion == nil {
				return serviceError.NewResourceNotFoundError("sink version", k.SinkID.String()+"/"+to.String(), "source")
			}
			return nil
		}).
		// Prepare the new value
		BeforeWrite(func(context.Context) {
			versionDescription := fmt.Sprintf(`Rollback to version "%d".`, targetVersion.Value.Version.Number)
			entity = targetVersion.Value
			entity.Version = latestVersion.Value.Version
			entity.IncrementVersion(entity, now, versionDescription)
		})

	// Save
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		return r.save(ctx, now, []definition.Sink{entity})
	})

	return atomicOp
}

// softDeleteAllFrom the parent key.
func (r *Repository) softDeleteAllFrom(parentKey fmt.Stringer, now time.Time, deletedWithParent bool) *op.AtomicOp[[]definition.Sink] {
	var all []definition.Sink
	atomicOp := op.Atomic(r.client, &all)

	// Get or list
	switch k := parentKey.(type) {
	case key.SinkKey:
		atomicOp.ReadOp(r.Get(k).WithOnResult(func(entity definition.Sink) {
			all = []definition.Sink{entity}
		}))
	default:
		atomicOp.ReadOp(r.List(parentKey).WithAllTo(&all))
	}

	// Mark deleted
	atomicOp.BeforeWrite(func(ctx context.Context) {
		for i := range all {
			v := &all[i]

			// Mark deleted
			v.Delete(now, deletedWithParent)
		}
	})

	// Save
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		return r.save(ctx, now, all)
	})

	return atomicOp
}

// undeleteAllFrom the parent key.
func (r *Repository) undeleteAllFrom(parentKey fmt.Stringer, now time.Time, undeletedWithParent bool) *op.AtomicOp[[]definition.Sink] {
	var all []definition.Sink
	atomicOp := op.Atomic(r.client, &all)

	// Get or list
	switch k := parentKey.(type) {
	case key.SinkKey:
		atomicOp.ReadOp(r.GetDeleted(k).WithOnResult(func(entity definition.Sink) {
			all = []definition.Sink{entity}
		}))
	default:
		atomicOp.ReadOp(r.ListDeleted(parentKey).WithAllTo(&all))
	}

	// Iterate all
	atomicOp.BeforeWrite(func(ctx context.Context) {
		for i := range all {
			v := &all[i]

			if v.DeletedWithParent != undeletedWithParent {
				continue
			}

			// Mark undeleted
			v.Undelete(now)

			// Create a new version record, if the entity has been undeleted manually
			if !undeletedWithParent {
				versionDescription := fmt.Sprintf(`Undeleted to version "%d".`, v.Version.Number)
				v.IncrementVersion(v, now, versionDescription)
			}
		}
	})

	// Save
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		return r.save(ctx, now, all)
	})

	return atomicOp
}

//nolint:dupl // similar to SourceRepository.save
func (r *Repository) save(ctx context.Context, now time.Time, all []definition.Sink) (op.Op, error) {
	saveCtx := plugin2.NewSaveContext(now)
	for _, v := range all {
		// Call plugins
		r.plugins.Executor().OnSinkSave(saveCtx, &v)

		if v.Deleted {
			// Move entity from the active prefix to the deleted prefix
			saveCtx.AddOp(
				// Delete entity from the active prefix
				r.schema.Active().ByKey(v.SinkKey).Delete(r.client),
				// Save entity to the deleted prefix
				r.schema.Deleted().ByKey(v.SinkKey).Put(r.client, v),
			)
		} else {
			saveCtx.AddOp(
				// Save record to the "active" prefix
				r.schema.Active().ByKey(v.SinkKey).Put(r.client, v),
				// Save record to the versions history
				r.schema.Versions().Of(v.SinkKey).Version(v.VersionNumber()).Put(r.client, v),
			)

			if v.UndeletedAt != nil && v.UndeletedAt.Time().Equal(now) {
				// Delete record from the "deleted" prefix, if needed
				saveCtx.AddOp(r.schema.Deleted().ByKey(v.SinkKey).Delete(r.client))
			}
		}
	}

	return saveCtx.Apply(ctx)
}

func (r *Repository) checkMaxSinksPerSource(k key.SourceKey, newCount int64) op.Op {
	return r.schema.
		Active().InSource(k).Count(r.client).
		WithResultValidator(func(actualCount int64) error {
			if actualCount+newCount > MaxSinksPerSource {
				return serviceError.NewCountLimitReachedError("sink", MaxSinksPerSource, "source")
			}
			return nil
		})
}

func (r *Repository) checkMaxSinksVersionsPerSink(k key.SinkKey, newCount int64) op.Op {
	return r.schema.
		Versions().Of(k).Count(r.client).
		WithResultValidator(func(actualCount int64) error {
			if actualCount+newCount > MaxSinkVersionsPerSink {
				return serviceError.NewCountLimitReachedError("version", MaxSinkVersionsPerSink, "sink")
			}
			return nil
		})
}
