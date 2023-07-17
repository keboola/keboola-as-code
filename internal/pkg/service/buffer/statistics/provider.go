package statistics

import (
	"context"
	"fmt"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Provider interface {
	ReceiverStats(ctx context.Context, k key.ReceiverKey) (model.StatsByType, error)
	ExportStats(ctx context.Context, k key.ExportKey) (model.StatsByType, error)
	FileStats(ctx context.Context, k key.FileKey) (model.StatsByType, error)
	SliceStats(ctx context.Context, k key.SliceKey) (model.StatsByType, error)
}

type Providers struct {
	realtime *RealtimeProvider
	cachedL1 *CachedL1Provider
	cachedL2 *CachedL2Provider
}

func (p *Providers) Realtime() *RealtimeProvider {
	return p.realtime
}

func (p *Providers) CachedL1() *CachedL1Provider {
	return p.cachedL1
}

func (p *Providers) CachedL2() *CachedL2Provider {
	return p.cachedL2
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	Schema() *schema.Schema
}

func NewProviders(d dependencies, ttlL2 time.Duration) (p *Providers, err error) {
	p = &Providers{}
	p.realtime = NewRealtimeProvider(d)
	if p.cachedL1, err = NewCachedL1Provider(d); err != nil {
		return nil, err
	}
	if p.cachedL2, err = NewCachedL2Provider(p.cachedL1, ttlL2, d); err != nil {
		return nil, err
	}
	return p, nil
}

func newGetters(fn getStatsFn) *getters {
	return &getters{getStatsFn: fn}
}

type getters struct {
	getStatsFn getStatsFn
}

type getStatsFn func(ctx context.Context, objectKey fmt.Stringer) (model.StatsByType, error)

func (v *getters) ReceiverStats(ctx context.Context, k key.ReceiverKey) (model.StatsByType, error) {
	return v.getStatsFn(ctx, k)
}

func (v *getters) ExportStats(ctx context.Context, k key.ExportKey) (model.StatsByType, error) {
	return v.getStatsFn(ctx, k)
}

func (v *getters) FileStats(ctx context.Context, k key.FileKey) (model.StatsByType, error) {
	return v.getStatsFn(ctx, k)
}

func (v *getters) SliceStats(ctx context.Context, k key.SliceKey) (model.StatsByType, error) {
	return v.getStatsFn(ctx, k)
}

func aggregate(state slicestate.State, partial model.Stats, out *model.StatsByType) {
	switch state {
	case slicestate.Writing, slicestate.Closing:
		out.Opened = out.Opened.Add(partial)
		out.AggregatedTotal = out.AggregatedTotal.Add(partial)
		out.AggregatedInBuffer = out.AggregatedInBuffer.Add(partial)
	case slicestate.Uploading:
		out.Uploading = out.Uploading.Add(partial)
		out.AggregatedTotal = out.AggregatedTotal.Add(partial)
		out.AggregatedInBuffer = out.AggregatedInBuffer.Add(partial)
	case slicestate.Failed:
		out.UploadFailed = out.UploadFailed.Add(partial)
		out.AggregatedTotal = out.AggregatedTotal.Add(partial)
		out.AggregatedInBuffer = out.AggregatedInBuffer.Add(partial)
	case slicestate.Uploaded:
		out.Uploaded = out.Uploaded.Add(partial)
		out.AggregatedTotal = out.AggregatedTotal.Add(partial)
	case slicestate.Imported:
		out.Imported = out.Imported.Add(partial)
		out.AggregatedTotal = out.AggregatedTotal.Add(partial)
	default:
		panic(errors.Errorf(`unexpected slice state "%v"`, state))
	}
}
