package source

import (
	"context"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/source/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
)

type Repository struct {
	client   *etcd.Client
	schema   schema.Source
	plugins  *plugin.Plugins
	branches *branch.Repository
}

type dependencies interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
	Plugins() *plugin.Plugins
}

func NewRepository(d dependencies, branches *branch.Repository) *Repository {
	r := &Repository{
		client:   d.EtcdClient(),
		schema:   schema.New(d.EtcdSerde()),
		plugins:  d.Plugins(),
		branches: branches,
	}

	r.disableSourcesOnBranchDisable()
	r.enableSourcesOnBranchEnable()
	r.deleteSourcesOnBranchDelete()
	r.undeleteSourcesOnBranchUndelete()

	return r
}

// save Source on create or update, triggers connected plugins to enrich the operation.
func (r *Repository) save(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Source) *op.TxnOp[definition.Source] {
	// Call plugins
	if err := r.plugins.Executor().OnSourceSave(ctx, now, by, old, updated); err != nil {
		return op.ErrorTxn[definition.Source](err)
	}

	saveTxn := op.TxnWithResult(r.client, updated)
	if updated.IsDeleted() {
		// Move entity from the active prefix to the deleted prefix
		saveTxn.Then(
			// Delete entity from the active prefix
			r.schema.Active().ByKey(updated.SourceKey).Delete(r.client),
			// Save entity to the deleted prefix
			r.schema.Deleted().ByKey(updated.SourceKey).Put(r.client, *updated),
		)
	} else {
		saveTxn.Then(
			// Save record to the "active" prefix
			r.schema.Active().ByKey(updated.SourceKey).Put(r.client, *updated),
			// Save record to the versions history
			r.schema.Versions().Of(updated.SourceKey).Version(updated.VersionNumber()).Put(r.client, *updated),
		)

		if updated.IsUndeletedAt(now) {
			// Delete record from the "deleted" prefix, if needed
			saveTxn.Then(r.schema.Deleted().ByKey(updated.SourceKey).Delete(r.client))
		}
	}

	return saveTxn
}

func (r *Repository) update(k key.SourceKey, now time.Time, by definition.By, versionDescription string, updateFn func(definition.Source) (definition.Source, error)) *op.AtomicOp[definition.Source] {
	var old, updated definition.Source
	return op.Atomic(r.client, &updated).
		// Check prerequisites
		Read(func(ctx context.Context) op.Op {
			return r.checkMaxSourcesVersionsPerSource(k, 1)
		}).
		// Read the entity
		Read(func(ctx context.Context) op.Op {
			return r.Get(k).WithResultTo(&old)
		}).
		// Update the entity
		Write(func(ctx context.Context) op.Op {
			var err error
			updated = deepcopy.Copy(old).(definition.Source)
			updated, err = updateFn(updated)
			if err != nil {
				return op.ErrorOp(err)
			}

			updated.IncrementVersion(updated, now, by, versionDescription)
			return r.save(ctx, now, by, &old, &updated)
		})
}
