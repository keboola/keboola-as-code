package diffop

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type executor struct {
	*Plan
	logger       log.Logger
	localManager *local.Manager
	localWork    *local.UnitOfWork
	remoteWork   *remote.UnitOfWork
	errors       errors.MultiError
}

func newExecutor(plan *Plan, logger log.Logger, ctx context.Context, localManager *local.Manager, remoteManager *remote.Manager, changeDescription string) *executor {
	return &executor{
		Plan:         plan,
		logger:       logger,
		localManager: localManager,
		localWork:    localManager.NewUnitOfWork(ctx),
		remoteWork:   remoteManager.NewUnitOfWork(ctx, changeDescription),
		errors:       errors.NewMultiError(),
	}
}

func (e *executor) invoke() error {
	// Validate
	if err := e.Validate(); err != nil {
		return err
	}
	e.logger.Debugf("Execution plan is valid.")

	// Invoke
	for _, action := range e.actions {
		switch action.action {
		case ActionSaveLocal:
			e.localWork.SaveObject(action.ObjectState, action.RemoteState(), action.ChangedFields)
		case ActionDeleteLocal:
			e.localWork.DeleteObject(action.ObjectState, action.Manifest())
		case ActionSaveRemote:
			e.remoteWork.SaveObject(action.ObjectState, action.LocalState(), action.ChangedFields)
		case ActionDeleteRemote:
			if e.allowedRemoteDelete {
				e.remoteWork.DeleteObject(action.ObjectState)
			}
		default:
			panic(errors.New(`unexpected action type`))
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

	return e.errors.ErrorOrNil()
}
