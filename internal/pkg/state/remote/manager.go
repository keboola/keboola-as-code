package remote

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
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
			for _, branch := range *response.Result().(*[]*model.Branch) {
				metadataRequest := u.branchMetadataRequest(branch, pool)
				response.WaitFor(metadataRequest)
				metadataRequest.Send()
			}
		}).
		OnSuccess(func(response *client.Response) {
			// Process branch + load branch components
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
	// Load metadata for configurations
	metadataMap, metadataReq := u.configsMetadataRequest(branch, pool)

	// Load components, configs and rows
	componentsReq := pool.
		Request(u.api.ListComponentsRequest(branch.Id)).
		OnSuccess(func(response *client.Response) {
			components := *response.Result().(*[]*model.ComponentWithConfigs)

			// Save component, it contains all configs and rows
			for _, component := range components {
				// Configs
				for _, config := range component.Configs {
					// Set config metadata
					metadata, found := metadataMap[config.ConfigKey]
					if !found {
						metadata = make(map[string]string)
					}
					config.Metadata = metadata

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
		})

	// Process response after the metadata is loaded
	componentsReq.WaitFor(metadataReq)

	// Send requests
	metadataReq.Send()
	componentsReq.Send()
}

func (u *UnitOfWork) branchMetadataRequest(branch *model.Branch, pool *client.Pool) *client.Request {
	request := pool.
		Request(u.api.ListBranchMetadataRequest(branch.Id)).
		OnSuccess(func(response *client.Response) {
			metadataResponse := *response.Result().(*[]storageapi.Metadata)
			branch.Metadata = make(map[string]string)
			for _, m := range metadataResponse {
				branch.Metadata[m.Key] = m.Value
			}
		})
	return request
}

func (u *UnitOfWork) configsMetadataRequest(branch *model.Branch, pool *client.Pool) (map[model.Key]map[string]string, *client.Request) {
	lock := &sync.Mutex{}
	out := make(map[model.Key]map[string]string)
	request := pool.
		Request(u.api.ListConfigMetadataRequest(branch.Id)).
		OnSuccess(func(response *client.Response) {
			lock.Lock()
			defer lock.Unlock()
			metadataResponse := *response.Result().(*storageapi.ConfigMetadataResponse)
			for key, metadata := range metadataResponse.MetadataMap(branch.Id) {
				metadataMap := make(map[string]string)
				for _, m := range metadata {
					metadataMap[m.Key] = m.Value
				}
				out[key] = metadataMap
			}
		})
	return out, request
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
		if objectState.GetRelativePath() == "" {
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

	// Should metadata be set?
	exists := objectState.HasRemoteState()
	setMetadata := !exists || changedFields.Has("metadata")
	var setMetadataReq *client.Request
	if setMetadata {
		setMetadataReq = u.api.AppendMetadataRequest(object)
		changedFields.Remove("metadata")
		// If there is no other change, send the request and return
		if len(changedFields) == 0 {
			setMetadataReq.Send()
			return nil
		}
	}

	// Create or update
	var createOrUpdateReq *client.Request
	if exists {
		// Update
		if r, err := u.updateRequest(objectState, object, recipe, changedFields); err != nil {
			return err
		} else {
			createOrUpdateReq = r
		}
	} else {
		// Create
		if r, err := u.createRequest(objectState, object, recipe); err != nil {
			return err
		} else {
			createOrUpdateReq = r
		}
	}

	// Set metadata
	if setMetadataReq != nil {
		// Set metadata if save has been successful
		createOrUpdateReq.OnSuccess(func(response *client.Response) {
			response.Sender().Send(setMetadataReq) // use same pool
		})
	}

	// Send
	createOrUpdateReq.Send()
	return nil
}

func (u *UnitOfWork) createRequest(objectState model.ObjectState, object model.Object, recipe *model.RemoteSaveRecipe) (*client.Request, error) {
	request, err := u.api.CreateRequest(recipe.Object)
	if err != nil {
		return nil, err
	}

	return u.poolFor(object.Level()).
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
					if r, err := u.updateRequest(objectState, object, recipe, nil); err != nil {
						response.SetErr(err)
					} else {
						response.SetErr(nil)
						response.WaitFor(r)
						r.Send()
					}
				}
			}
		}), nil
}

func (u *UnitOfWork) updateRequest(objectState model.ObjectState, object model.Object, recipe *model.RemoteSaveRecipe, changedFields model.ChangedFields) (*client.Request, error) {
	request, err := u.api.UpdateRequest(recipe.Object, changedFields)
	if err != nil {
		return nil, err
	}

	return u.poolFor(object.Level()).
		Request(request).
		OnSuccess(func(response *client.Response) {
			objectState.SetRemoteState(object)
			u.changes.AddUpdated(objectState)
		}), nil
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
