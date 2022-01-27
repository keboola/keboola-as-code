package rename

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
)

type executor struct {
	*Plan
	*local.Manager
	ctx context.Context
}

func newRenameExecutor(ctx context.Context, localManager *local.Manager, plan *Plan) *executor {
	return &executor{Plan: plan, Manager: localManager, ctx: ctx}
}

func (e *executor) invoke() error {
	uow := e.NewUnitOfWork(e.ctx)
	uow.Rename(e.actions)
	return uow.Invoke()
}
