package rename

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
)

type executor struct {
	*Plan
	*local.Manager
	ctx     context.Context
	options Options
}

func newRenameExecutor(ctx context.Context, localManager *local.Manager, plan *Plan, options Options) *executor {
	return &executor{Plan: plan, Manager: localManager, ctx: ctx, options: options}
}

func (e *executor) invoke() error {
	uow := e.NewUnitOfWork(e.ctx)
	if e.options.Cleanup {
		uow.EnableRenameCleanup()
	}
	uow.Rename(e.actions)
	return uow.Invoke()
}
