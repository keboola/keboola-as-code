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
		remoteWork:   plan.State.RemoteManager().NewUnitOfWork(plan.changeDescription),
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
			e.localWork.SaveObject(action.ObjectState)
		case ActionDeleteLocal:
			e.localWork.DeleteObject(action.ObjectState)
		case ActionSaveRemote:
			if err := e.remoteWork.SaveObject(action.ObjectState, action.ChangedFields); err != nil {
				e.errors.Append(err)
			}
		case ActionDeleteRemote:
			if e.allowedRemoteDelete {
				if err := e.remoteWork.DeleteObject(action.ObjectState); err != nil {
					e.errors.Append(err)
				}
			}
		default:
			panic(fmt.Errorf(`unexpected action type`))
		}
	}

	// Invoke pools for each level (branches, configs, rows) separately
	if err := e.remoteWork.Invoke(); err != nil {
		e.errors.Append(err)
	}

	// Wait for workers
	if err := e.localWork.Wait(); err != nil {
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
