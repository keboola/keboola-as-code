package sink

import (
	"context"
	"fmt"
	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/sink/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/source"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
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
	plugins *plugin.Plugins
	sources *source.Repository
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
}

func NewRepository(d dependencies, sources *source.Repository) *Repository {
	r := &Repository{
		client:  d.EtcdClient(),
		schema:  schema.ForSink(d.EtcdSerde()),
		plugins: d.Plugins(),
		sources: sources,
	}

	// Delete/undelete source with branch
	r.plugins.Collection().OnSourceSave(func(ctx *plugin.SaveContext, old, updated *definition.Source) {
		if updated.DeletedAt != nil && updated.DeletedAt.Time().Equal(ctx.Now()) {
			ctx.AddAtomicOp(r.softDeleteAllFrom(updated.SourceKey, ctx.Now(), true))
		} else if updated.UndeletedAt != nil && updated.UndeletedAt.Time().Equal(ctx.Now()) {
			ctx.AddAtomicOp(r.undeleteAllFrom(updated.SourceKey, ctx.Now(), true))
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
	var created definition.Sink
	var actual, deleted *op.KeyValueT[definition.Sink]

	atomicOp := op.Atomic(r.client, &created).
		// Check prerequisites
		ReadOp(r.sources.ExistsOrErr(k.SourceKey)).
		ReadOp(r.checkMaxSinksPerSource(k.SourceKey, 1)).
		// Get gets actual version to check if the entity already exists
		ReadOp(r.schema.Active().ByKey(k).GetKV(r.client).WithResultTo(&actual)).
		// GetDelete gets deleted version to check if we have to do undelete
		ReadOp(r.schema.Deleted().ByKey(k).GetKV(r.client).WithResultTo(&deleted)).
		// Create
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			// Entity must not exist
			if actual != nil {
				return nil, serviceError.NewResourceAlreadyExistsError("sink", k.SinkID.String(), "source")
			}

			// Create on undelete
			created = deepcopy.Copy(*input).(definition.Sink)
			if deleted != nil {
				created.Version = deleted.Value.Version
				created.SoftDeletable = deleted.Value.SoftDeletable
				created.Undelete(now)
			}

			// Save
			created.IncrementVersion(created, now, versionDescription)
			return r.saveOne(ctx, now, nil, &created)
		}).
		// Update the input entity after successful operation
		OnResult(func(entity definition.Sink) {
			*input = entity
		})

	return atomicOp
}

func (r *Repository) Update(k key.SinkKey, now time.Time, versionDescription string, updateFn func(definition.Sink) (definition.Sink, error)) *op.AtomicOp[definition.Sink] {
	var old, updated definition.Sink
	return op.Atomic(r.client, &updated).
		// Check prerequisites
		ReadOp(r.checkMaxSinksVersionsPerSink(k, 1)).
		// Read the entity
		ReadOp(r.Get(k).WithResultTo(&old)).
		// Update the entity
		WriteOrErr(func(ctx context.Context) (op op.Op, err error) {
			// Update
			updated = deepcopy.Copy(old).(definition.Sink)
			updated, err = updateFn(updated)
			if err != nil {
				return nil, err
			}

			// Save
			updated.IncrementVersion(updated, now, versionDescription)
			return r.saveOne(ctx, now, &old, &updated)
		})
}

func (r *Repository) SoftDelete(k key.SinkKey, now time.Time) *op.AtomicOp[definition.Sink] {
	var deleted definition.Sink
	return op.Atomic(r.client, &deleted).
		AddFrom(r.
			softDeleteAllFrom(k, now, false).
			OnResult(func(r []definition.Sink) {
				if len(r) == 1 {
					deleted = r[0]
				}
			}))
}

func (r *Repository) Undelete(k key.SinkKey, now time.Time) *op.AtomicOp[definition.Sink] {
	var undeleted definition.Sink
	return op.Atomic(r.client, &undeleted).
		// Check prerequisites
		ReadOp(r.sources.ExistsOrErr(k.SourceKey)).
		// Read the entity
		ReadOp(r.checkMaxSinksPerSource(k.SourceKey, 1)).
		// Mark undeleted
		AddFrom(r.
			undeleteAllFrom(k, now, false).
			OnResult(func(r []definition.Sink) {
				if len(r) == 1 {
					undeleted = r[0]
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
	var updated definition.Sink
	var latestVersion, targetVersion *op.KeyValueT[definition.Sink]
	return op.Atomic(r.client, &updated).
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
		WriteOrErr(func(ctx context.Context) (op.Op, error) {
			versionDescription := fmt.Sprintf(`Rollback to version "%d".`, targetVersion.Value.Version.Number)
			old := targetVersion.Value
			updated = deepcopy.Copy(old).(definition.Sink)
			updated.Version = latestVersion.Value.Version
			updated.IncrementVersion(updated, now, versionDescription)
			return r.saveOne(ctx, now, &old, &updated)
		})
}

// softDeleteAllFrom the parent key.
func (r *Repository) softDeleteAllFrom(parentKey fmt.Stringer, now time.Time, deletedWithParent bool) *op.AtomicOp[[]definition.Sink] {
	var allOld, allDeleted []definition.Sink
	atomicOp := op.Atomic(r.client, &allDeleted)

	// Get or list
	switch k := parentKey.(type) {
	case key.SinkKey:
		atomicOp.ReadOp(r.Get(k).WithOnResult(func(entity definition.Sink) { allOld = []definition.Sink{entity} }))
	default:
		atomicOp.ReadOp(r.List(parentKey).WithAllTo(&allOld))
	}

	// Iterate all
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		saveCtx := plugin.NewSaveContext(now)
		for _, old := range allOld {
			// Mark deleted
			deleted := deepcopy.Copy(old).(definition.Sink)
			deleted.Delete(now, deletedWithParent)

			// Save
			r.save(saveCtx, &old, &deleted)
			allDeleted = append(allDeleted, deleted)
		}
		return saveCtx.Apply(ctx)
	})

	return atomicOp
}

// undeleteAllFrom the parent key.
func (r *Repository) undeleteAllFrom(parentKey fmt.Stringer, now time.Time, undeletedWithParent bool) *op.AtomicOp[[]definition.Sink] {
	var allOld, allCreated []definition.Sink
	atomicOp := op.Atomic(r.client, &allCreated)

	// Get or list
	switch k := parentKey.(type) {
	case key.SinkKey:
		atomicOp.ReadOp(r.GetDeleted(k).WithOnResult(func(entity definition.Sink) { allOld = []definition.Sink{entity} }))
	default:
		atomicOp.ReadOp(r.ListDeleted(parentKey).WithAllTo(&allOld))
	}

	// Iterate all
	atomicOp.WriteOrErr(func(ctx context.Context) (op.Op, error) {
		saveCtx := plugin.NewSaveContext(now)
		for _, old := range allOld {
			if old.DeletedWithParent != undeletedWithParent {
				continue
			}

			// Mark undeleted
			created := deepcopy.Copy(old).(definition.Sink)
			created.Undelete(now)

			// Create a new version record, if the entity has been undeleted manually
			if !undeletedWithParent {
				versionDescription := fmt.Sprintf(`Undeleted to version "%d".`, old.Version.Number)
				created.IncrementVersion(created, now, versionDescription)
			}

			// Save
			r.save(saveCtx, nil, &created)
			allCreated = append(allCreated, created)
		}
		return saveCtx.Apply(ctx)
	})

	return atomicOp
}

func (r *Repository) saveOne(ctx context.Context, now time.Time, old, updated *definition.Sink) (op.Op, error) {
	saveCtx := plugin.NewSaveContext(now)
	r.save(saveCtx, old, updated)
	return saveCtx.Apply(ctx)
}

func (r *Repository) save(saveCtx *plugin.SaveContext, old, updated *definition.Sink) {
	// Call plugins
	r.plugins.Executor().OnSinkSave(saveCtx, old, updated)

	if updated.Deleted {
		// Move entity from the active prefix to the deleted prefix
		saveCtx.AddOp(
			// Delete entity from the active prefix
			r.schema.Active().ByKey(updated.SinkKey).Delete(r.client),
			// Save entity to the deleted prefix
			r.schema.Deleted().ByKey(updated.SinkKey).Put(r.client, *updated),
		)
	} else {
		saveCtx.AddOp(
			// Save record to the "active" prefix
			r.schema.Active().ByKey(updated.SinkKey).Put(r.client, *updated),
			// Save record to the versions history
			r.schema.Versions().Of(updated.SinkKey).Version(updated.VersionNumber()).Put(r.client, *updated),
		)

		if updated.UndeletedAt != nil && updated.UndeletedAt.Time().Equal(saveCtx.Now()) {
			// Delete record from the "deleted" prefix, if needed
			saveCtx.AddOp(r.schema.Deleted().ByKey(updated.SinkKey).Delete(r.client))
		}
	}
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
