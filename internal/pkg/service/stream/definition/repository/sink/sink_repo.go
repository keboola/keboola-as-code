package sink

import (
	"context"
	"time"

	"github.com/keboola/go-utils/pkg/deepcopy"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/sink/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/source"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
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

	r.disableSinksOnSourceDisable()
	r.enableSinksOnSourceEnable()
	r.deleteSinksOnSourceDelete()
	r.undeleteSinksOnSourceUndelete()

	return r
}

// save Sink on create or update, triggers connected plugins to enrich the operation.
func (r *Repository) save(ctx context.Context, now time.Time, by definition.By, old, updated *definition.Sink) op.Op {
	// Call plugins
	r.plugins.Executor().OnSinkSave(ctx, now, by, old, updated)

	saveTxn := op.Txn(r.client)
	if updated.IsDeleted() {
		// Move entity from the active prefix to the deleted prefix
		saveTxn.Then(
			// Delete entity from the active prefix
			r.schema.Active().ByKey(updated.SinkKey).Delete(r.client),
			// Save entity to the deleted prefix
			r.schema.Deleted().ByKey(updated.SinkKey).Put(r.client, *updated),
		)
	} else {
		saveTxn.Then(
			// Save record to the "active" prefix
			r.schema.Active().ByKey(updated.SinkKey).Put(r.client, *updated),
			// Save record to the versions history
			r.schema.Versions().Of(updated.SinkKey).Version(updated.VersionNumber()).Put(r.client, *updated),
		)

		if updated.IsUndeletedAt(now) {
			// Delete record from the "deleted" prefix, if needed
			saveTxn.Then(r.schema.Deleted().ByKey(updated.SinkKey).Delete(r.client))
		}
	}
	return saveTxn
}

func (r *Repository) update(k key.SinkKey, now time.Time, by definition.By, versionDescription string, updateFn func(definition.Sink) (definition.Sink, error)) *op.AtomicOp[definition.Sink] {
	var old, updated definition.Sink
	return op.Atomic(r.client, &updated).
		// Check prerequisites
		ReadOp(r.checkMaxSinksVersionsPerSink(k, 1)).
		// Read the entity
		ReadOp(r.Get(k).WithResultTo(&old)).
		// Update the entity
		WriteOrErr(func(ctx context.Context) (op op.Op, err error) {
			updated = deepcopy.Copy(old).(definition.Sink)
			updated, err = updateFn(updated)
			if err != nil {
				return nil, err
			}

			// Save
			updated.IncrementVersion(updated, now, by, versionDescription)
			return r.save(ctx, now, by, &old, &updated), nil
		})
}
