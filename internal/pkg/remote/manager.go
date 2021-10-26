package remote

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/iancoleman/orderedmap"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/client"
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type Manager struct {
	state        *model.State
	localManager *local.Manager
	api          *StorageApi
	mapper       *mapper.Mapper
}

type UnitOfWork struct {
	*Manager
	ctx               context.Context
	lock              *sync.Mutex
	changeDescription string                 // change description used for all modified configs and rows
	pools             *orderedmap.OrderedMap // separated pool for changes in branches, configs and rows
	newObjects        []model.Object
	errors            *utils.Error
	invoked           bool
}

func NewManager(localManager *local.Manager, api *StorageApi, state *model.State, mapper *mapper.Mapper) *Manager {
	return &Manager{
		state:        state,
		localManager: localManager,
		api:          api,
		mapper:       mapper,
	}
}

func (m *Manager) Manifest() *manifest.Manifest {
	return m.localManager.Manifest()
}

func (m *Manager) NewUnitOfWork(ctx context.Context, changeDescription string) *UnitOfWork {
	return &UnitOfWork{
		Manager:           m,
		ctx:               ctx,
		lock:              &sync.Mutex{},
		changeDescription: changeDescription,
		pools:             utils.NewOrderedMap(),
		errors:            utils.NewMultiError(),
	}
}

func (u *UnitOfWork) LoadAll() {
	// Run all requests in one pool
	pool := u.poolFor(-1)

	// Branches
	pool.
		Request(u.api.ListBranchesRequest()).
		OnSuccess(func(response *client.Response) {
			// Save branch + load branch components
			for _, branch := range *response.Result().(*[]*model.Branch) {
				// Store branch to state
				if objectState, err := u.loadObject(branch); err != nil {
					u.errors.Append(err)
					continue
				} else if objectState == nil {
					// Ignored -> skip
					continue
				}

				// Load components
				u.loadBranch(branch, pool)
			}
		}).
		Send()
}

func (u *UnitOfWork) loadBranch(branch *model.Branch, pool *client.Pool) {
	pool.
		Request(u.api.ListComponentsRequest(branch.Id)).
		OnSuccess(func(response *client.Response) {
			components := *response.Result().(*[]*model.ComponentWithConfigs)

			// Save component, it contains all configs and rows
			for _, component := range components {
				// Configs
				for _, config := range component.Configs {
					// Store config to state
					if objectState, err := u.loadObject(config.Config); err != nil {
						u.errors.Append(err)
						continue
					} else if objectState == nil {
						// Ignored -> skip
						continue
					}

					// Rows
					for _, row := range config.Rows {
						//  Store row to state
						if _, err := u.loadObject(row); err != nil {
							u.errors.Append(err)
							continue
						}
					}
				}
			}
		}).
		Send()
}

func (u *UnitOfWork) loadObject(object model.Object) (model.ObjectState, error) {
	// Skip ignored objects
	if u.Manifest().IsObjectIgnored(object) {
		return nil, nil
	}

	// Invoke mapper
	recipe := &model.RemoteLoadRecipe{Original: object, Modified: object.Clone()}
	if err := u.mapper.AfterRemoteLoad(recipe); err != nil {
		return nil, err
	}
	object = recipe.Modified

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
	objectState.SetRemoteState(object)

	// Generate local path
	if !found {
		if err := u.localManager.UpdatePaths(objectState, false); err != nil {
			return nil, err
		}
	}

	u.addNewObject(object)
	return objectState, nil
}

func (u *UnitOfWork) SaveObject(objectState model.ObjectState, object model.Object, changedFields []string) {
	if v, ok := objectState.(*model.BranchState); ok && v.Remote == nil {
		// Branch cannot be created from the CLI
		u.errors.Append(fmt.Errorf(`branch "%d" (%s) exists only locally, new branch cannot be created by CLI`, v.Local.Id, v.Local.Name))
		return
	}

	// Invoke mapper
	recipe := &model.RemoteSaveRecipe{Original: object, Modified: object.Clone()}
	if err := u.mapper.BeforeRemoteSave(recipe); err != nil {
		u.errors.Append(err)
		return
	}

	if err := u.createOrUpdate(objectState, recipe, changedFields); err != nil {
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

	u.pools.SortKeys(sort.Strings)
	for _, level := range u.pools.Keys() {
		pool, _ := u.pools.Get(level)
		if err := pool.(*client.Pool).StartAndWait(); err != nil {
			u.errors.Append(err)
			break
		}
	}

	u.invoked = true
	return u.errors.ErrorOrNil()
}

func (u *UnitOfWork) createOrUpdate(objectState model.ObjectState, recipe *model.RemoteSaveRecipe, changedFields []string) error {
	// Set changeDescription
	switch v := recipe.Modified.(type) {
	case *model.Config:
		v.ChangeDescription = u.changeDescription
		if len(changedFields) > 0 {
			changedFields = append(changedFields, "changeDescription")
		}
	case *model.ConfigRow:
		v.ChangeDescription = u.changeDescription
		if len(changedFields) > 0 {
			changedFields = append(changedFields, "changeDescription")
		}
	}

	// Create or update
	if !objectState.HasRemoteState() {
		// Create
		return u.create(objectState, recipe)
	}

	// Update
	return u.update(objectState, recipe, changedFields)
}

func (u *UnitOfWork) create(objectState model.ObjectState, recipe *model.RemoteSaveRecipe) error {
	object := recipe.Modified
	request, err := u.api.CreateRequest(object)
	if err != nil {
		return err
	}
	u.poolFor(object.Level()).
		Request(request).
		OnSuccess(func(response *client.Response) {
			// Save new ID to manifest
			objectState.SetRemoteState(recipe.Original)
		}).
		OnError(func(response *client.Response) {
			if e, ok := response.Error().(*Error); ok {
				if e.ErrCode == "configurationAlreadyExists" || e.ErrCode == "configurationRowAlreadyExists" {
					// Object exists -> update instead of create + clear error
					response.SetErr(u.update(objectState, recipe, nil))
				}
			}
		}).
		Send()
	return nil
}

func (u *UnitOfWork) update(objectState model.ObjectState, recipe *model.RemoteSaveRecipe, changedFields []string) error {
	object := recipe.Modified
	if request, err := u.api.UpdateRequest(object, changedFields); err == nil {
		u.poolFor(object.Level()).
			Request(request).
			OnSuccess(func(response *client.Response) {
				objectState.SetRemoteState(recipe.Original)
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
			u.Manifest().DeleteRecord(objectState)
			objectState.SetRemoteState(nil)
		}).
		Send()
}

func (u *UnitOfWork) addNewObject(object model.Object) {
	u.lock.Lock()
	defer u.lock.Unlock()
	u.newObjects = append(u.newObjects, object)
}

// poolFor each level (branches, configs, rows).
func (u *UnitOfWork) poolFor(level int) *client.Pool {
	if u.invoked {
		panic(fmt.Errorf(`invoked UnitOfWork cannot be reused`))
	}

	key := cast.ToString(level)
	if value, found := u.pools.Get(key); found {
		return value.(*client.Pool)
	}

	pool := u.api.NewPool()
	pool.SetContext(u.ctx)
	u.pools.Set(key, pool)
	return pool
}
