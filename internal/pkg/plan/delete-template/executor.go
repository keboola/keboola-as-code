package delete_template

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type executor struct {
	*Plan
	ctx context.Context
}

func newExecutor(ctx context.Context, plan *Plan) *executor {
	return &executor{Plan: plan, ctx: ctx}
}

func (e *executor) invoke() error {
	uow := e.projectState.LocalManager().NewUnitOfWork(e.ctx)
	for _, action := range e.actions {
		uow.DeleteObject(action.State, action.Manifest)
	}

	branchState := e.Plan.projectState.GetOrNil(e.Plan.branchKey).(*model.BranchState)
	if err := branchState.Local.Metadata.DeleteTemplateUsage(e.Plan.instanceId); err != nil {
		return utils.PrefixError(`cannot remove template instance metadata`, err)
	}
	uow.SaveObject(branchState, branchState.LocalState(), model.NewChangedFields())

	return uow.Invoke()
}
