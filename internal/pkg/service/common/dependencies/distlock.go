package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distlock"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// distributedLockScope implements DistributedLockScope interface.
type distributedLockScope struct {
	provider *distlock.Provider
}

type distributedLockScopeDeps interface {
	BaseScope
	EtcdClientScope
}

func NewDistributedLockScope(ctx context.Context, cfg distlock.Config, d distributedLockScopeDeps) (DistributedLockScope, error) {
	return newDistributedLockScope(ctx, cfg, d)
}

func newDistributedLockScope(ctx context.Context, cfg distlock.Config, d distributedLockScopeDeps) (v *distributedLockScope, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.common.dependencies.NewDistributedLockScope")
	defer span.End(&err)

	provider, err := distlock.NewProvider(ctx, cfg, d)
	if err != nil {
		return nil, err
	}

	return &distributedLockScope{provider: provider}, nil
}

func (v *distributedLockScope) check() {
	if v == nil {
		panic(errors.New("dependencies distributed lock scope is not initialized"))
	}
}

func (v *distributedLockScope) DistributedLockProvider() *distlock.Provider {
	v.check()
	return v.provider
}
