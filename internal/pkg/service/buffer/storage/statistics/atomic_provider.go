package statistics

import (
	"context"
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
)

type AtomicProvider struct {
	*getters
	client *etcd.Client
	prefix schemaRoot
}

type atomicProviderDeps interface {
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
}

func NewAtomicProvider(d atomicProviderDeps) *AtomicProvider {
	p := &AtomicProvider{
		client: d.EtcdClient(),
		prefix: newSchema(d.EtcdSerde()),
	}

	// Setup common getters
	p.getters = newGetters(p.statsFromDB)

	return p
}

func (p *AtomicProvider) statsFromDB(ctx context.Context, objectKey fmt.Stringer) (out Aggregated, err error) {
	var ops []op.Op

	for _, level := range storage.AllLevels() {
		level := level

		// Get stats prefix for the slice state
		pfx := p.prefix.InLevel(level).InObject(objectKey)

		// Sum
		ops = append(ops, pfx.GetAll().ForEachOp(func(v Value, header *iterator.Header) error {
			aggregate(level, v, &out)
			return nil
		}))
	}

	// Wrap all get operations to a transaction
	if err := op.MergeToTxn(ops...).DoOrErr(ctx, p.client); err != nil {
		return out, err
	}

	return out, nil
}
