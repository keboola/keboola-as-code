package statistics

import (
	"context"
	"fmt"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
)

type RealtimeProvider struct {
	*getters
	logger log.Logger
	client *etcd.Client
	prefix schema.Stats
}

type realtimeProviderDeps interface {
	Logger() log.Logger
	EtcdClient() *etcd.Client
	Schema() *schema.Schema
}

func NewRealtimeProvider(d realtimeProviderDeps) *RealtimeProvider {
	p := &RealtimeProvider{
		logger: d.Logger().AddPrefix("[stats-realtime]"),
		client: d.EtcdClient(),
		prefix: d.Schema().SliceStats(),
	}

	// Setup common getters
	p.getters = newGetters(p.statsFromDB)

	return p
}

func (p *RealtimeProvider) statsFromDB(ctx context.Context, objectKey fmt.Stringer) (out model.StatsByType, err error) {
	var ops []op.Op

	for _, state := range allStates {
		state := state

		// Get stats prefix for the slice state
		pfx := p.prefix.InState(state).InObject(objectKey)

		// Sum
		ops = append(ops, pfx.GetAll().ForEachOp(func(v model.Stats, header *iterator.Header) error {
			aggregate(state, v, &out)
			return nil
		}))
	}

	// Wrap all get operations to a transaction
	if err := op.MergeToTxn(ops...).DoOrErr(ctx, p.client); err != nil {
		return out, err
	}

	return out, nil
}
