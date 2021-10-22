package remote

import (
	"context"
	"fmt"
	"sort"

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
	localManager *local.Manager
	api          *StorageApi
	mapper       *mapper.Mapper
}

type UnitOfWork struct {
	*Manager
	ctx               context.Context
	changeDescription string                 // change description used for all modified configs and rows
	pools             *orderedmap.OrderedMap // separated pool for changes in branches, configs and rows
	errors            *utils.Error
	invoked           bool
}

type objectsState interface {
	SetRemoteState(remote model.Object) (model.ObjectState, error)
}

func NewManager(localManager *local.Manager, api *StorageApi, mapper *mapper.Mapper) *Manager {
	return &Manager{
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
		changeDescription: changeDescription,
		pools:             utils.NewOrderedMap(),
		errors:            utils.NewMultiError(),
	}
}

func (u *UnitOfWork) LoadAllTo(state objectsState) {
	// Run all requests in one pool
	pool := u.poolFor(-1)

	// Branches
	pool.
		Request(u.api.ListBranchesRequest()).
		OnSuccess(func(response *client.Response) {
			// Save branch + load branch components
			for _, branch := range *response.Result().(*[]*model.Branch) {
				// Store branch to state
				if objectState, err := u.loadObjectTo(branch, state); err != nil {
					u.errors.Append(err)
					continue
				} else if objectState == nil {
					// Ignored -> skip
					continue
				}

				// Load components
				u.loadBranchTo(branch, state, pool)
			}
		}).
		Send()
}

func (u *UnitOfWork) loadBranchTo(branch *model.Branch, state objectsState, pool *client.Pool) {
	pool.
		Request(u.api.ListComponentsRequest(branch.Id)).
		OnSuccess(func(response *client.Response) {
			components := *response.Result().(*[]*model.ComponentWithConfigs)

			// Save component, it contains all configs and rows
			for _, component := range components {
				// Configs
				for _, config := range component.Configs {
					// Store config to state
					if objectState, err := u.loadObjectTo(config.Config, state); err != nil {
						u.errors.Append(err)
						continue
					} else if objectState == nil {
						// Ignored -> skip
						continue
					}

					// Rows
					for _, row := range config.Rows {
						//  Store row to state
						if _, err := u.loadObjectTo(row, state); err != nil {
							u.errors.Append(err)
							continue
						}
					}
				}
			}
		}).
		Send()
}

func (u *UnitOfWork) loadObjectTo(object model.Object, state objectsState) (model.ObjectState, error) {
	// Invoke mapper
	if err := u.mapper.AfterRemoteLoad(&model.RemoteLoadRecipe{Object: object}); err != nil {
		return nil, err
	}

	// Set remote state
	return state.SetRemoteState(object)
}

func (u *UnitOfWork) SaveObject(object model.ObjectState, changedFields []string) {
	switch v := object.(type) {
	case *model.BranchState:
		if v.Remote != nil {
			if err := u.createOrUpdate(v, changedFields); err != nil {
				u.errors.Append(err)
			}
		} else {
			// Branch cannot be created from the CLI
			u.errors.Append(fmt.Errorf(`branch "%d" (%s) exists only locally, new branch cannot be created by CLI`, v.Local.Id, v.Local.Name))
			return
		}
	case *model.ConfigState:
		if err := u.createOrUpdate(v, changedFields); err != nil {
			u.errors.Append(err)
		}
	case *model.ConfigRowState:
		if err := u.createOrUpdate(v, changedFields); err != nil {
			u.errors.Append(err)
		}
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}

func (u *UnitOfWork) DeleteObject(object model.ObjectState) {
	switch v := object.(type) {
	case *model.BranchState:
		branch := v.LocalOrRemoteState().(*model.Branch)
		if branch.IsDefault {
			u.errors.Append(fmt.Errorf("default branch cannot be deleted"))
			return
		}

		// Branch must be deleted in blocking operation
		if _, err := u.api.DeleteBranch(branch.Id); err != nil {
			u.errors.Append(err)
			return
		}
	case *model.ConfigState:
		u.poolFor(v.Level()).
			Request(u.api.DeleteConfigRequest(v.Remote)).
			OnSuccess(func(response *client.Response) {
				u.Manifest().DeleteRecord(v)
				object.SetRemoteState(nil)
			}).
			Send()
	case *model.ConfigRowState:
		u.poolFor(v.Level()).
			Request(u.api.DeleteConfigRowRequest(v.Remote)).
			OnSuccess(func(response *client.Response) {
				u.Manifest().DeleteRecord(v)
				object.SetRemoteState(nil)
			}).
			Send()
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}

func (u *UnitOfWork) Invoke() error {
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

func (u *UnitOfWork) createOrUpdate(objectState model.ObjectState, changedFields []string) error {
	// Get object local state.
	// Remote state is used for marking object as deleted (then local state is not set)
	object := objectState.LocalOrRemoteState()

	// Set changeDescription
	switch v := object.(type) {
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
		return u.create(objectState, object)
	}

	// Update
	return u.update(objectState, object, changedFields)
}

func (u *UnitOfWork) create(objectState model.ObjectState, object model.Object) error {
	request, err := u.api.CreateRequest(object)
	if err != nil {
		return err
	}
	u.poolFor(object.Level()).
		Request(request).
		OnSuccess(func(response *client.Response) {
			// Save new ID to manifest
			objectState.SetRemoteState(object)
		}).
		OnError(func(response *client.Response) {
			if e, ok := response.Error().(*Error); ok {
				if e.ErrCode == "configurationAlreadyExists" || e.ErrCode == "configurationRowAlreadyExists" {
					// Object exists -> update instead of create + clear error
					response.SetErr(u.update(objectState, object, nil))
				}
			}
		}).
		Send()
	return nil
}

func (u *UnitOfWork) update(objectState model.ObjectState, object model.Object, changedFields []string) error {
	if request, err := u.api.UpdateRequest(object, changedFields); err == nil {
		u.poolFor(object.Level()).
			Request(request).
			OnSuccess(func(response *client.Response) {
				objectState.SetRemoteState(object)
			}).
			Send()
	} else {
		return err
	}
	return nil
}

// poolFor each level (branches, configs, rows).
func (u *UnitOfWork) poolFor(level int) *client.Pool {
	if u.invoked {
		panic(`invoked UnitOfWork cannot be reused`)
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
