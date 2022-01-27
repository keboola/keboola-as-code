package remote

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/client"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type Manager struct {
	state        model.ObjectStates
	localManager *local.Manager
	api          *storageapi.Api
	mapper       *mapper.Mapper
}

type UnitOfWork struct {
	*Manager
	ctx               context.Context
	lock              *sync.Mutex
	changeDescription string                 // change description used for all modified configs and rows
	storageApiPools   *orderedmap.OrderedMap // separated pool for changes in branches, configs and rows
	changes           *model.RemoteChanges
	errors            *utils.MultiError
	invoked           bool
}

func NewManager(localManager *local.Manager, api *storageapi.Api, objects model.ObjectStates, mapper *mapper.Mapper) *Manager {
	return &Manager{
		state:        objects,
		localManager: localManager,
		api:          api,
		mapper:       mapper,
	}
}

func (m *Manager) Manifest() manifest.Manifest {
	return m.localManager.Manifest()
}

func (m *Manager) NewUnitOfWork(ctx context.Context, changeDescription string) *UnitOfWork {
	return &UnitOfWork{
		Manager:           m,
		ctx:               ctx,
		lock:              &sync.Mutex{},
		changeDescription: changeDescription,
		storageApiPools:   orderedmap.New(),
		changes:           model.NewRemoteChanges(),
		errors:            utils.NewMultiError(),
	}
}

func (u *UnitOfWork) LoadAll(filter model.ObjectsFilter) {
	// Run all requests in one pool
	pool := u.poolFor(-1)

	// Branches
	pool.
		Request(u.api.ListBranchesRequest()).
		OnSuccess(func(response *client.Response) {
			// Save branch + load branch components
			for _, branch := range *response.Result().(*[]*model.Branch) {
				// Store branch to state
				if objectState, err := u.loadObject(branch, filter); err != nil {
					u.errors.Append(err)
					continue
				} else if objectState == nil {
					// Ignored -> skip
					continue
				}

				// Load components
				u.loadBranch(branch, filter, pool)
			}
		}).
		Send()
}

func (u *UnitOfWork) loadBranch(branch *model.Branch, filter model.ObjectsFilter, pool *client.Pool) {
	pool.
		Request(u.api.ListComponentsRequest(branch.Id)).
		OnSuccess(func(response *client.Response) {
			components := *response.Result().(*[]*model.ComponentWithConfigs)

			// Save component, it contains all configs and rows
			for _, component := range components {
				// Configs
				for _, config := range component.Configs {
					// Store config to state
					if objectState, err := u.loadObject(config.Config, filter); err != nil {
						u.errors.Append(err)
						continue
					} else if objectState == nil {
						// Ignored -> skip
						continue
					}

					// Rows
					for _, row := range config.Rows {
						//  Store row to state
						if _, err := u.loadObject(row, filter); err != nil {
							u.errors.Append(err)
							continue
						}
					}
				}
			}
		}).
		Send()
}

func (u *UnitOfWork) loadObject(object model.Object, filter model.ObjectsFilter) (model.ObjectState, error) {
	// Skip ignored objects
	if filter.IsObjectIgnored(object) {
		return nil, nil
	}

	// Get object state
	objectState, found := u.state.Get(object.Key())

	// Create object state if needed
	if !found {
		// Create manifest record
		record, _, err := u.Manifest().CreateOrGetRecord(object.Key())
		if err != nil {
			return nil, err
		}

		// Create object state
		objectState, err = u.state.CreateFrom(record)
		if err != nil {
			return nil, err
		}
	}

	// Set remote state
	internalObject := deepcopy.Copy(object).(model.Object)
	objectState.SetRemoteState(internalObject)

	// Invoke mapper
	recipe := model.NewRemoteLoadRecipe(objectState.Manifest(), internalObject)
	if err := u.mapper.MapAfterRemoteLoad(recipe); err != nil {
		return nil, err
	}

	u.changes.AddLoaded(objectState)
	return objectState, nil
}

func (u *UnitOfWork) SaveObject(objectState model.ObjectState, object model.Object, changedFields model.ChangedFields) {
	if v, ok := objectState.(*model.BranchState); ok && v.Remote == nil {
		// Branch cannot be created from the CLI
		u.errors.Append(fmt.Errorf(`branch "%d" (%s) exists only locally, new branch cannot be created by CLI`, v.Local.Id, v.Local.Name))
		return
	}

	// Invoke mapper
	apiObject := deepcopy.Copy(object).(model.Object)
	recipe := model.NewRemoteSaveRecipe(objectState.Manifest(), apiObject, changedFields)
	if err := u.mapper.MapBeforeRemoteSave(recipe); err != nil {
		u.errors.Append(err)
		return
	}

	if err := u.createOrUpdate(objectState, object, recipe, changedFields); err != nil {
		u.errors.Append(err)
	}
}

func (u *UnitOfWork) DeleteObject(objectState model.ObjectState) {
	if v, ok := objectState.(*model.BranchState); ok {
		branch := v.LocalOrRemoteState().(*model.Branch)
		if branch.IsDefault {
			u.errors.Append(fmt.Errorf("default branch cannot be deleted"))
			return
		}

		// Branch must be deleted in blocking operation
		if _, err := u.api.DeleteBranch(branch.BranchKey); err != nil {
			u.errors.Append(err)
		}

		return
	}

	u.delete(objectState)
}

func (u *UnitOfWork) Invoke() error {
	if u.invoked {
		panic(fmt.Errorf(`invoked UnitOfWork cannot be reused`))
	}

	// Start and wait for all pools
	u.storageApiPools.SortKeys(sort.Strings)
	for _, level := range u.storageApiPools.Keys() {
		pool, _ := u.storageApiPools.Get(level)
		if err := pool.(*client.Pool).StartAndWait(); err != nil {
			u.errors.Append(err)
			break
		}
	}

	// AfterRemoteOperation event
	if !u.changes.Empty() {
		if err := u.mapper.AfterRemoteOperation(u.changes); err != nil {
			u.errors.Append(err)
		}
	}

	// Generate local path if needed
	pathsUpdater := u.localManager.NewPathsGenerator(false)
	for _, objectState := range u.changes.Loaded() {
		if objectState.GetObjectPath() == "" {
			pathsUpdater.Add(objectState)
		}
	}
	if err := pathsUpdater.Invoke(); err != nil {
		u.errors.Append(err)
	}

	u.invoked = true
	return u.errors.ErrorOrNil()
}

func (u *UnitOfWork) createOrUpdate(objectState model.ObjectState, object model.Object, recipe *model.RemoteSaveRecipe, changedFields model.ChangedFields) error {
	// Set changeDescription
	switch v := recipe.Object.(type) {
	case *model.Config:
		v.ChangeDescription = u.changeDescription
		changedFields.Add("changeDescription")
	case *model.ConfigRow:
		v.ChangeDescription = u.changeDescription
		changedFields.Add("changeDescription")
	}

	// Create or update
	if !objectState.HasRemoteState() {
		// Create
		return u.create(objectState, object, recipe)
	}

	// Update
	return u.update(objectState, object, recipe, changedFields)
}

func (u *UnitOfWork) create(objectState model.ObjectState, object model.Object, recipe *model.RemoteSaveRecipe) error {
	request, err := u.api.CreateRequest(recipe.Object)
	if err != nil {
		return err
	}
	u.poolFor(object.Level()).
		Request(request).
		OnSuccess(func(response *client.Response) {
			// Save new ID to manifest
			objectState.SetRemoteState(object)
			u.changes.AddCreated(objectState)
		}).
		OnError(func(response *client.Response) {
			if e, ok := response.Error().(*storageapi.Error); ok {
				if e.ErrCode == "configurationAlreadyExists" || e.ErrCode == "configurationRowAlreadyExists" {
					// Object exists -> update instead of create + clear error
					response.SetErr(u.update(objectState, object, recipe, nil))
				}
			}
		}).
		Send()
	return nil
}

func (u *UnitOfWork) update(objectState model.ObjectState, object model.Object, recipe *model.RemoteSaveRecipe, changedFields model.ChangedFields) error {
	if request, err := u.api.UpdateRequest(recipe.Object, changedFields); err == nil {
		u.poolFor(object.Level()).
			Request(request).
			OnSuccess(func(response *client.Response) {
				objectState.SetRemoteState(object)
				u.changes.AddUpdated(objectState)
			}).
			Send()
	} else {
		return err
	}
	return nil
}

func (u *UnitOfWork) delete(objectState model.ObjectState) {
	u.poolFor(objectState.Level()).
		Request(u.api.DeleteRequest(objectState.Key())).
		OnSuccess(func(response *client.Response) {
			u.Manifest().Delete(objectState)
			objectState.SetRemoteState(nil)
		}).
		OnSuccess(func(response *client.Response) {
			u.changes.AddDeleted(objectState)
		}).
		Send()
}

// poolFor each level (branches, configs, rows).
func (u *UnitOfWork) poolFor(level int) *client.Pool {
	if u.invoked {
		panic(fmt.Errorf(`invoked UnitOfWork cannot be reused`))
	}

	key := cast.ToString(level)
	if value, found := u.storageApiPools.Get(key); found {
		return value.(*client.Pool)
	}

	pool := u.api.NewPool()
	pool.SetContext(u.ctx)
	u.storageApiPools.Set(key, pool)
	return pool
}
