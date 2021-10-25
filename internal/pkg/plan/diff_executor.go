package plan

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type diffExecutor struct {
	*DiffPlan
	logger       *zap.SugaredLogger
	localManager *local.Manager
	localWork    *local.UnitOfWork
	remoteWork   *remote.UnitOfWork
	errors       *utils.Error
}

func newDiffExecutor(plan *DiffPlan, logger *zap.SugaredLogger, ctx context.Context) *diffExecutor {
	return &diffExecutor{
		DiffPlan:     plan,
		logger:       logger,
		localManager: plan.State.LocalManager(),
		localWork:    plan.State.LocalManager().NewUnitOfWork(ctx),
		remoteWork:   plan.State.RemoteManager().NewUnitOfWork(ctx, plan.changeDescription),
		errors:       utils.NewMultiError(),
	}
}

func (e *diffExecutor) invoke() error {
	// Validate
	if err := e.Validate(); err != nil {
		return err
	}
	e.logger.Debugf("Execution plan is valid.")

	// Invoke
	for _, action := range e.actions {
		switch action.action {
		case ActionSaveLocal:
			e.localWork.SaveObject(action.ObjectState, action.RemoteState())
		case ActionDeleteLocal:
			e.localWork.DeleteObject(action.ObjectState, action.Manifest())
		case ActionSaveRemote:
			e.remoteWork.SaveObject(action.ObjectState, action.LocalState(), action.ChangedFields)
		case ActionDeleteRemote:
			if e.allowedRemoteDelete {
				e.remoteWork.DeleteObject(action.ObjectState)
			}
		default:
			panic(fmt.Errorf(`unexpected action type`))
		}
	}

	// Invoke pools for each level (branches, configs, rows) separately
	if err := e.remoteWork.Invoke(); err != nil {
		e.errors.Append(err)
	}

	// Invoke local workers
	if err := e.localWork.Invoke(); err != nil {
		e.errors.Append(err)
	}

	// Delete invalid objects (eg. if pull --force used, and work continued even an invalid state found)
	if err := e.localManager.DeleteInvalidObjects(); err != nil {
		e.errors.Append(err)
	}

	// Delete empty directories, eg. no extractor of a type left -> dir is empty
	if err := e.localManager.DeleteEmptyDirectories(e.State.TrackedPaths()); err != nil {
		e.errors.Append(err)
	}

	return e.errors.ErrorOrNil()
}
