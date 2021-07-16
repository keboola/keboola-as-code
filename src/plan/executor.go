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
		errors:   utils.NewMultiError(),
		workers:  group,
		pools:    utils.NewOrderedMap(),
	}
}

func (e *Executor) Invoke(p *Plan) error {
	// Validate
	if err := p.Validate(); err != nil {
		return utils.PrefixError(fmt.Sprintf("cannot perform the \"%s\" operation", p.Name), err)
	}
	e.logger.Debugf("Execution plan is valid.")

	// Invoke
	e.errors = utils.NewMultiError()
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
			if p.allowedRemoteDelete {
				e.deleteRemote(action.Result)
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
			e.errors.Append(err)
			break
		}
	}

	// Wait for workers
	if err := e.workers.Wait(); err != nil {
		e.errors.Append(err)
	}

	// Delete invalid objects (eg. if pull --force used, and work continued even an invalid state found)
	records := e.manifest.GetRecords()
	for _, key := range append([]string(nil), records.Keys()...) {
		v, _ := records.Get(key)
		record := v.(manifest.Record)
		if record.State().IsInvalid() {
			if err := local.DeleteModel(e.logger, e.manifest, record); err != nil {
				e.errors.Append(err)
			}
		}
	}

	// Delete empty directories
	if err := local.DeleteEmptyDirectories(e.logger, e.manifest.ProjectDir); err != nil {
		e.errors.Append(err)
	}

	return e.errors.ErrorOrNil()
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
			e.errors.Append(err)
		}
		return nil
	})
}

func (e *Executor) deleteLocal(object state.ObjectState) {
	e.workers.Go(func() error {
		err := local.DeleteModel(e.logger, e.manifest, object.Manifest())
		if err != nil {
			e.errors.Append(err)
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
		// Create sequentially, branches cannot be created in parallel
		e.api.
			CreateBranchRequest(branch.Local).
			OnSuccess(func(response *client.Response) {
				// Save new ID to manifest
				branch.Local = branch.Remote
				result.ObjectState.UpdateManifest(e.manifest, false)
				e.saveLocal(branch)
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
		e.errors.Append(err)
	}
}

func (e *Executor) saveConfig(config *state.ConfigState, result *diff.Result) {
	pool := e.getPoolFor(config.Level())
	if config.Local.Id == "" {
		// Create
		request, err := e.api.CreateConfigRequest(&model.ConfigWithRows{Config: config.Local})
		if err != nil {
			e.errors.Append(err)
			return
		}
		pool.
			Request(request).
			OnSuccess(func(response *client.Response) {
				// Save new ID to manifest
				config.Local = config.Remote
				result.ObjectState.UpdateManifest(e.manifest, false)
				e.saveLocal(config)
			}).
			Send()
	} else if config.Remote != nil {
		// Update
		request, err := e.api.UpdateConfigRequest(config.Local, result.ChangedFields)
		if err != nil {
			e.errors.Append(err)
			return
		}
		pool.
			Request(request).
			Send()
	} else {
		// Restore deleted -> not possible
		e.errors.Append(fmt.Errorf("TODO"))
	}
}

func (e *Executor) saveConfigRow(row *state.ConfigRowState, result *diff.Result) {
	pool := e.getPoolFor(row.Level())
	if row.Local.Id == "" {
		// No ID -> not present in remote -> create
		request, err := e.api.CreateConfigRowRequest(row.Local)
		if err != nil {
			e.errors.Append(err)
			return
		}
		pool.
			Request(request).
			OnSuccess(func(response *client.Response) {
				// Save new ID to manifest
				row.Local = row.Remote
				result.ObjectState.UpdateManifest(e.manifest, false)
				e.saveLocal(row)
			}).
			Send()
	} else if row.Remote != nil {
		// Local ID + present in remote -> update
		request, err := e.api.UpdateConfigRowRequest(row.Local, result.ChangedFields)
		if err != nil {
			e.errors.Append(err)
			return
		}
		pool.
			Request(request).
			Send()
	} else {
		// Restore deleted -> not possible
		e.errors.Append(fmt.Errorf("TODO"))
	}
}

func (e *Executor) deleteRemote(result *diff.Result) {
	switch v := result.ObjectState.(type) {
	case *state.BranchState:
		e.manifest.DeleteRecord(v)
		// Delete sequentially, branches cannot be deleted in parallel
		_, err := e.api.DeleteBranch(v.Id)
		if err != nil {
			e.errors.Append(err)
		}
	case *state.ConfigState:
		e.manifest.DeleteRecord(v)
		pool := e.getPoolFor(v.Level())
		pool.
			Request(e.api.DeleteConfigRequest(v.ComponentId, v.Id)).
			Send()
	case *state.ConfigRowState:
		e.manifest.DeleteRecord(v)
		pool := e.getPoolFor(v.Level())
		pool.
			Request(e.api.DeleteConfigRowRequest(v.ComponentId, v.ConfigId, v.Id)).
			Send()
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, result.State))
	}
}
