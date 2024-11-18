package plugin

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	targetModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/model"
)

type throttleFn func(ctx context.Context, sinkKey key.SinkKey) bool

type Throttle struct {
	SinkKey  key.SinkKey
	Provider targetModel.Provider
}

func (p *Plugins) RegisterThrottle(provider targetModel.Provider, fn throttleFn) {
	p.throttle[provider] = fn
}

func (p *Plugins) IsThrottled(ctx context.Context, provider targetModel.Provider, sinkKey key.SinkKey) bool {
	if fn, ok := p.throttle[provider]; ok {
		return fn(ctx, sinkKey)
	}

	return false
}
