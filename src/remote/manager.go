package remote

import (
	"fmt"
	"sort"

	"keboola-as-code/src/client"
	"keboola-as-code/src/local"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"

	"github.com/iancoleman/orderedmap"
	"github.com/spf13/cast"
)

type Manager struct {
	localManager *local.Manager
	api          *StorageApi
}

type UnitOfWork struct {
	*Manager
	changeDescription string                 // change description used for all modified configs and rows
	pools             *orderedmap.OrderedMap // separated pool for changes in branches, configs and rows
	errors            *utils.Error
	invoked           bool
}

func NewManager(localManager *local.Manager, api *StorageApi) *Manager {
	return &Manager{
		localManager: localManager,
		api:          api,
	}
}

func (m *Manager) Manifest() *manifest.Manifest {
	return m.localManager.Manifest()
}

func (m *Manager) NewUnitOfWork(changeDescription string) *UnitOfWork {
	return &UnitOfWork{
		Manager:           m,
		changeDescription: changeDescription,
		pools:             utils.NewOrderedMap(),
		errors:            utils.NewMultiError(),
	}
}

func (u *UnitOfWork) SaveRemote(object model.ObjectState, changedFields []string) error {
	switch v := object.(type) {
	case *model.BranchState:
		if v.Remote != nil {
			return u.createOrUpdate(v, changedFields)
		} else {
			// Branch cannot be created from the CLI
			return fmt.Errorf(`branch "%d" (%s) exists only locally, new branch cannot be created by CLI`, v.Local.Id, v.Local.Name)
		}
	case *model.ConfigState:
		return u.createOrUpdate(v, changedFields)
	case *model.ConfigRowState:
		return u.createOrUpdate(v, changedFields)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}

func (u *UnitOfWork) DeleteRemote(object model.ObjectState) error {
	switch v := object.(type) {
	case *model.BranchState:
		return fmt.Errorf(`branch (%d - %s) cannot be deleted by CLI`, v.Local.Id, v.Local.Name)
	case *model.ConfigState:
		u.Manifest().DeleteRecord(v)
		u.poolFor(v.Level()).
			Request(u.api.DeleteConfigRequest(v.ComponentId, v.Id)).
			Send()
	case *model.ConfigRowState:
		u.Manifest().DeleteRecord(v)
		u.poolFor(v.Level()).
			Request(u.api.DeleteConfigRowRequest(v.ComponentId, v.ConfigId, v.Id)).
			Send()
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}

	return nil
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

func (u *UnitOfWork) createOrUpdate(object model.ObjectState, changedFields []string) error {
	// Set changeDescription
	switch v := object.(type) {
	case *model.ConfigState:
		v.Local.ChangeDescription = u.changeDescription
		changedFields = append(changedFields, "changeDescription")
	case *model.ConfigRowState:
		v.Local.ChangeDescription = u.changeDescription
		changedFields = append(changedFields, "changeDescription")
	}

	// Create or update
	if !object.HasRemoteState() {
		// Create
		request, err := u.api.CreateRequest(object.LocalState())
		if err != nil {
			return err
		}
		u.poolFor(object.Level()).
			Request(request).
			OnSuccess(func(response *client.Response) {
				// Save new ID to manifest
				object.SetRemoteState(object.LocalState())
				u.localManager.UpdatePaths(object, false)
				if err := u.localManager.SaveModel(object.Manifest(), object.LocalState()); err != nil {
					u.errors.Append(err)
				}
			}).
			Send()
	} else {
		// Update
		if request, err := u.api.UpdateRequest(object.LocalState(), changedFields); err == nil {
			u.poolFor(object.Level()).
				Request(request).
				OnSuccess(func(response *client.Response) {
					object.SetRemoteState(object.LocalState())
				}).
				Send()
		} else {
			return err
		}
	}
	return nil
}

// poolFor each level (branches, configs, rows)
func (u *UnitOfWork) poolFor(level int) *client.Pool {
	if u.invoked {
		panic(`invoked UnitOfWork cannot be reused`)
	}

	key := cast.ToString(level)
	if value, found := u.pools.Get(key); found {
		return value.(*client.Pool)
	}

	pool := u.api.NewPool()
	u.pools.Set(key, pool)
	return pool
}
