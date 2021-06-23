package plan

import (
	"context"
	"fmt"
	"github.com/iancoleman/orderedmap"
	"github.com/spf13/cast"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"keboola-as-code/src/client"
	"keboola-as-code/src/diff"
	"keboola-as-code/src/local"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
	"sort"
)

type Executor struct {
	logger   *zap.SugaredLogger
	ctx      context.Context
	api      *remote.StorageApi
	manifest *manifest.Manifest
	errors   *utils.Error
	workers  *errgroup.Group
	pools    *orderedmap.OrderedMap
}

func NewExecutor(logger *zap.SugaredLogger, ctx context.Context, api *remote.StorageApi, manifest *manifest.Manifest) *Executor {
	group, _ := errgroup.WithContext(ctx)
	return &Executor{
		logger:   logger,
		ctx:      ctx,
		api:      api,
		manifest: manifest,
		errors:   &utils.Error{},
		workers:  group,
		pools:    utils.NewOrderedMap(),
	}
}

func (e *Executor) Invoke(p *Plan) error {
	e.errors = &utils.Error{}
	e.workers, _ = errgroup.WithContext(e.ctx)

	for _, action := range p.Actions {
		switch action.Type {
		case ActionSaveLocal:
			if err := e.saveLocal(action.Result); err != nil {
				e.errors.Add(err)
			}
		case ActionSaveRemote:
			if err := e.saveRemote(action.Result); err != nil {
				e.errors.Add(err)
			}
		case ActionDeleteLocal:
			if err := e.deleteLocal(action.Result); err != nil {
				e.errors.Add(err)
			}
		case ActionDeleteRemote:
			if err := e.deleteRemote(action.Result); err != nil {
				e.errors.Add(err)
			}
		default:
			panic(fmt.Errorf("unexpected action type"))
		}
	}

	// Invoke pools for each level (branches, configs, rows) separately
	e.pools.SortKeys(sort.Strings)
	for _, level := range e.pools.Keys() {
		pool, _ := e.pools.Get(level)
		if err := pool.(*client.Pool).StartAndWait(); err != nil {
			e.errors.Add(err)
			break
		}
	}

	// Wait for workers
	if err := e.workers.Wait(); err != nil {
		e.errors.Add(err)
	}

	// Delete empty directories
	if err := local.DeleteEmptyDirectories(e.logger, e.manifest.ProjectDir); err != nil {
		e.errors.Add(err)
	}

	if e.errors.Len() > 0 {
		return fmt.Errorf("pull failed: %s", e.errors)
	}

	return nil
}

func (e *Executor) getPoolFor(level int) *client.Pool {
	key := cast.ToString(level)
	if value, found := e.pools.Get(key); found {
		return value.(*client.Pool)
	}

	pool := e.api.NewPool()
	e.pools.Set(key, pool)
	return pool
}

func (e *Executor) saveLocal(object state.ObjectState) error {
	e.workers.Go(func() error {
		return local.SaveModel(e.logger, e.manifest, object.Manifest(), object.RemoteState())
	})
	return nil
}

func (e *Executor) deleteLocal(object state.ObjectState) error {
	e.workers.Go(func() error {
		return local.DeleteModel(e.logger, e.manifest, object.Manifest(), object.LocalState())
	})
	return nil
}

func (e *Executor) saveRemote(result *diff.Result) error {
	switch v := result.ObjectState.(type) {
	case *state.BranchState:
		return e.saveBranch(v, result)
	case *state.ConfigState:
		return e.saveConfig(v, result)
	case *state.ConfigRowState:
		return e.saveConfigRow(v, result)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, result.State))
	}
}

func (e *Executor) saveBranch(branch *state.BranchState, result *diff.Result) error {
	pool := e.getPoolFor(branch.Level())
	if branch.Local.Id == 0 {
		// Create - sequentially, branches cannot be created in parallel
		e.api.
			CreateBranchRequest(branch.Local).
			OnSuccess(func(response *client.Response) *client.Response {
				// Save new branch ID to manifest
				branch.Remote = branch.Local
				branch.BranchManifest.BranchKey = branch.Remote.BranchKey
				if err := e.saveLocal(branch); err != nil {
					e.errors.Add(err)
				}
				return response
			}).
			Send()
	} else if branch.Remote != nil {
		// Update
		pool.
			Request(e.api.UpdateBranchRequest(branch.Local, result.ChangedFields)).
			Send()
	} else {
		// Restore deleted -> not possible
		return fmt.Errorf(`branch "%d" (%s) exists only locally, it cannot be restored or recreated with the same ID`, branch.Local.Id, branch.Local.Name)
	}
	return nil
}

func (e *Executor) saveConfig(config *state.ConfigState, result *diff.Result) error {
	//pool := e.getPoolFor(config.Level())
	return nil
}

func (e *Executor) saveConfigRow(row *state.ConfigRowState, result *diff.Result) error {
	//pool := e.getPoolFor(row.Level())
	return nil
}

func (e *Executor) deleteRemote(result *diff.Result) error {
	return fmt.Errorf("TODO REMOTE DELETE")
}
