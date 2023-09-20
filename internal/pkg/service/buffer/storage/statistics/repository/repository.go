package repository

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	etcd "go.etcd.io/etcd/client/v3"
)

const (
	// putMaxStatsPerTxn defines maximum number of keys per transaction when updating database values
	putMaxStatsPerTxn = 100
)

type atomicProvider = AtomicProvider

type Repository struct {
	*atomicProvider
	telemetry telemetry.Telemetry
	client    *etcd.Client
	schema    schemaRoot
}

type repositoryDeps interface {
	Telemetry() telemetry.Telemetry
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
}

func NewRepository(provider *AtomicProvider, d repositoryDeps) *Repository {
	return &Repository{
		atomicProvider: NewAtomicProvider(d),
		telemetry:      d.Telemetry(),
		client:         d.EtcdClient(),
		schema:         newSchema(d.EtcdSerde()),
	}
}

func (r *Repository) AtomicProvider() *AtomicProvider {
	return r.atomicProvider
}

func (r *Repository) MoveOp(ctx context.Context, sliceKey storage.SliceKey, from, to storage.Level, modifyStatsFn func(*Value)) (op.Op, error) {
	if from == to {
		panic(errors.Errorf(`from and to categories are same and equal to "%s"`, to))
	}

	fromKey := r.schema.InLevel(from).InSlice(sliceKey)
	toKey := r.schema.InLevel(to).InSlice(sliceKey)

	stats, err := fromKey.Get().Do(ctx, r.client)
	if err != nil {
		return nil, err
	}

	if modifyStatsFn != nil {
		modifyStatsFn(&stats.Value)
	}

	return op.MergeToTxn(fromKey.Delete(), toKey.Put(stats.Value)), nil
}
