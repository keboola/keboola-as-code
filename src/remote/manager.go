package remote

import (
	"fmt"
	"sort"
	"strings"

	"github.com/iancoleman/orderedmap"
	"github.com/spf13/cast"

	"keboola-as-code/src/client"
	"keboola-as-code/src/local"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
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
		u.poolFor(v.Level()).
			Request(u.api.DeleteConfigRequest(v.ComponentId, v.Id)).
			OnSuccess(func(response *client.Response) {
				u.Manifest().DeleteRecord(v)
				object.SetRemoteState(nil)
			}).
			Send()
	case *model.ConfigRowState:
		u.poolFor(v.Level()).
			Request(u.api.DeleteConfigRowRequest(v.ComponentId, v.ConfigId, v.Id)).
			OnSuccess(func(response *client.Response) {
				u.Manifest().DeleteRecord(v)
				object.SetRemoteState(nil)
			}).
			Send()
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}

	return nil
}

// MarkDeleted config or row from dev branch. Prefix is added at the name beginning.
func (u *UnitOfWork) MarkDeleted(object model.ObjectState) error {
	switch o := object.RemoteState().(type) {
	case *model.Config:
		o.Name = model.ToDeletePrefix + strings.TrimPrefix(o.Name, model.ToDeletePrefix)
	case *model.ConfigRow:
		o.Name = model.ToDeletePrefix + strings.TrimPrefix(o.Name, model.ToDeletePrefix)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}

	// Save
	object.SetLocalState(object.RemoteState())
	changedFields := []string{"name", "changeDescription"}
	return u.SaveRemote(object, changedFields)
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

			// If marking as deleted -> local state is not set
			if objectState.HasLocalState() {
				u.localManager.UpdatePaths(objectState, false)
				if err := u.localManager.SaveModel(objectState.Manifest(), objectState.LocalState()); err != nil {
					u.errors.Append(err)
				}
			}
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
	u.pools.Set(key, pool)
	return pool
}
