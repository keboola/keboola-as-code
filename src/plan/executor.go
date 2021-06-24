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
	"keboola-as-code/src/model"
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
			e.saveLocal(action.Result)
		case ActionSaveRemote:
			e.saveRemote(action.Result)
		case ActionDeleteLocal:
			e.deleteLocal(action.Result)
		case ActionDeleteRemote:
			e.deleteRemote(action.Result)
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

	// Delete invalid objects (if pull --force used)
	records := e.manifest.GetRecords()
	for _, key := range append([]string(nil), records.Keys()...) {
		v, _ := records.Get(key)
		record := v.(manifest.Record)
		if record.IsInvalid() {
			if err := local.DeleteModel(e.logger, e.manifest, record); err != nil {
				e.errors.Add(err)
			}
		}
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

func (e *Executor) saveLocal(object state.ObjectState) {
	e.workers.Go(func() error {
		err := local.SaveModel(e.logger, e.manifest, object.Manifest(), object.RemoteState())
		if err != nil {
			e.errors.Add(err)
		}
		return nil
	})
}

func (e *Executor) deleteLocal(object state.ObjectState) {
	e.workers.Go(func() error {
		err := local.DeleteModel(e.logger, e.manifest, object.Manifest())
		if err != nil {
			e.errors.Add(err)
		}
		return nil
	})
}

func (e *Executor) saveRemote(result *diff.Result) {
	switch v := result.ObjectState.(type) {
	case *state.BranchState:
		e.saveBranch(v, result)
	case *state.ConfigState:
		e.saveConfig(v, result)
	case *state.ConfigRowState:
		e.saveConfigRow(v, result)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, result.State))
	}
}

func (e *Executor) saveBranch(branch *state.BranchState, result *diff.Result) {
	pool := e.getPoolFor(branch.Level())
	if branch.Local.Id == 0 {
		// Create - sequentially, branches cannot be created in parallel
		e.api.
			CreateBranchRequest(branch.Local).
			OnSuccess(func(response *client.Response) *client.Response {
				// Save new ID to manifest
				branch.Local = branch.Remote
				result.ObjectState.UpdateManifest(e.manifest)
				e.saveLocal(branch)
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
		err := fmt.Errorf(`branch "%d" (%s) exists only locally, it cannot be restored or recreated with the same ID`, branch.Local.Id, branch.Local.Name)
		e.errors.Add(err)
	}
}

func (e *Executor) saveConfig(config *state.ConfigState, result *diff.Result) {
	pool := e.getPoolFor(config.Level())
	if config.Local.Id == "" {
		// Create
		request, err := e.api.CreateConfigRequest(&model.ConfigWithRows{Config: config.Local})
		if err != nil {
			e.errors.Add(err)
			return
		}
		pool.
			Request(request).
			OnSuccess(func(response *client.Response) *client.Response {
				// Save new ID to manifest
				config.Local = config.Remote
				result.ObjectState.UpdateManifest(e.manifest)
				e.saveLocal(config)
				return response
			}).
			Send()
	} else if config.Remote != nil {
		// Update
		request, err := e.api.UpdateConfigRequest(config.Local, result.ChangedFields)
		if err != nil {
			e.errors.Add(err)
			return
		}
		pool.
			Request(request).
			Send()
	} else {
		// Restore deleted -> not possible
		e.errors.Add(fmt.Errorf("TODO"))
	}
}

func (e *Executor) saveConfigRow(row *state.ConfigRowState, result *diff.Result) {
	//pool := e.getPoolFor(row.Level())
}

func (e *Executor) deleteRemote(result *diff.Result) {

}
