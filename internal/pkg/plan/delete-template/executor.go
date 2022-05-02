package delete_template

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
)

type executor struct {
	*Plan
	*local.Manager
	ctx context.Context
}

func newExecutor(ctx context.Context, localManager *local.Manager, plan *Plan) *executor {
	return &executor{Plan: plan, Manager: localManager, ctx: ctx}
}

func (e *executor) invoke() error {
	uow := e.NewUnitOfWork(e.ctx)
	for _, action := range e.actions {
		uow.DeleteObject(action.State, action.Manifest)
	}
	return uow.Invoke()
}
