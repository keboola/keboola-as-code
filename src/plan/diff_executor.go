package plan

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"keboola-as-code/src/local"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
)

type diffExecutor struct {
	*DiffPlan
	logger       *zap.SugaredLogger
	ctx          context.Context
	localManager *local.Manager
	localWorkers *errgroup.Group
	remoteWork   *remote.UnitOfWork
	errors       *utils.Error
}

func newDiffExecutor(plan *DiffPlan, logger *zap.SugaredLogger, api *remote.StorageApi, ctx context.Context) *diffExecutor {
	workers, _ := errgroup.WithContext(ctx)
	localManager := plan.State.LocalManager()
	return &diffExecutor{
		DiffPlan:     plan,
		logger:       logger,
		ctx:          ctx,
		localWorkers: workers,
		localManager: localManager,
		remoteWork:   remote.NewManager(localManager, api).NewUnitOfWork(plan.changeDescription),
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
			a := action
			e.addWorker(func() error {
				if err := e.localManager.SaveModel(a.Manifest(), a.RemoteState()); err != nil {
					return err
				}
				a.SetLocalState(a.RemoteState())
				return nil
			})
		case ActionDeleteLocal:
			a := action
			e.addWorker(func() error {
				if err := e.localManager.DeleteModel(a.Manifest()); err != nil {
					return err
				}
				a.SetLocalState(nil)
				return nil
			})
		case ActionSaveRemote:
			if err := e.remoteWork.SaveRemote(action.ObjectState, action.ChangedFields); err != nil {
				e.errors.Append(err)
			}
		case ActionDeleteRemote:
			if e.allowedRemoteDelete {
				if err := e.remoteWork.DeleteRemote(action.ObjectState); err != nil {
					e.errors.Append(err)
				}
			}
		case ActionMarkDeletedRemote:
			if e.allowedRemoteDelete {
				if err := e.remoteWork.MarkDeleted(action.ObjectState); err != nil {
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
	if err := e.localWorkers.Wait(); err != nil {
		e.errors.Append(err)
	}

	// Delete invalid objects (eg. if pull --force used, and work continued even an invalid state found)
	if err := e.localManager.DeleteInvalidObjects(); err != nil {
		e.errors.Append(err)
	}

	// Delete empty directories, eg. no extractor of an type left -> dir is empty
	if err := e.localManager.DeleteEmptyDirectories(e.State.TrackedPaths()); err != nil {
		e.errors.Append(err)
	}

	return e.errors.ErrorOrNil()
}

func (e *diffExecutor) addWorker(worker func() error) {
	e.localWorkers.Go(func() error {
		if err := worker(); err != nil {
			e.errors.Append(err)
		}
		return nil
	})
}
