package plan

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/local"
)

type renameExecutor struct {
	*RenamePlan
	*local.Manager
	ctx context.Context
}

func newRenameExecutor(ctx context.Context, localManager *local.Manager, plan *RenamePlan) *renameExecutor {
	return &renameExecutor{RenamePlan: plan, Manager: localManager, ctx: ctx}
}

func (e *renameExecutor) invoke() error {
	uow := e.NewUnitOfWork(e.ctx)
	uow.Rename(e.actions)
	return uow.Invoke()
}
