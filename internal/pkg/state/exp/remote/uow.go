package remote

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// UnitOfWork executed on remote.State in parallel when Invoke is called.
type UnitOfWork interface {
	Invoke() error
	LoadAll()
	Save(object model.Object, changedFields model.ChangedFields)
	Delete(key model.Key)
}

// uow implements UnitOfWork interface.
type uow struct {
	state *State
	// Inputs
	ctx               context.Context
	loadFilter        model.ObjectsFilter
	changeDescription string // change description used for all modified configs and rows
	// Internals
	invoked         bool
	storageApiPools *orderedmap.OrderedMap // separated pool for changes in branches, configs and rows
	changes         *model.Changes
	errors          *utils.MultiError
}

func newUnitOfWork(state *State, ctx context.Context, changeDescription string, loadFilter model.ObjectsFilter) UnitOfWork {
	return &uow{
		state:             state,
		ctx:               ctx,
		loadFilter:        loadFilter,
		changeDescription: changeDescription,
		storageApiPools:   orderedmap.New(),
		changes:           model.NewRemoteChanges(),
		errors:            utils.NewMultiError(),
	}
}

// Invoke work planned by other methods.
func (u *uow) Invoke() error {
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
		if err := u.state.mapper.AfterRemoteOperation(u.changes); err != nil {
			u.errors.Append(err)
		}
	}

	u.invoked = true
	return u.errors.ErrorOrNil()
}

// LoadAll remote objects according configured loadFilter.
func (u *uow) LoadAll() {
	// Run all requests in one pool
	pool := u.poolFor(-1)

	// Branches
	pool.
		Request(u.state.api.ListBranchesRequest()).
		OnSuccess(func(response *client.Response) {
			// Process branch + load branch components
			for _, branch := range *response.Result().(*[]*model.Branch) {
				// Store branch to state
				if objectState, err := u.addObject(branch); err != nil {
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

// Save remote object. Object will be created or updated.
func (u *uow) Save(object model.Object, changedFields model.ChangedFields) {
	if v, ok := object.(*model.Branch); ok {
		if _, exists := u.state.Get(object.Key()); !exists {
			// Branch cannot be created from the CLI
			u.errors.Append(fmt.Errorf(`branch "%d" (%s) cannot be created, it must be created as clone of the main branch directly in the project`, v.Id, v.Name))
			return
		}
	}

	// Validate
	if err := validator.Validate(u.ctx, object); err != nil {
		u.errors.AppendWithPrefix(fmt.Sprintf(`%s is invalid`, object.String()), err)
		return
	}

	// Invoke mapper
	apiObject := deepcopy.Copy(object).(model.Object)
	recipe := model.NewRemoteSaveRecipe(apiObject, changedFields)
	if err := u.state.mapper.MapBeforeRemoteSave(recipe); err != nil {
		u.errors.Append(err)
		return
	}

	// Create request
	request, exists, err := u.createOrUpdate(recipe, changedFields)
	if err != nil {
		u.errors.Append(err)
		return
	}

	// Update state
	request.OnSuccess(func(response *client.Response) {
		// Add to state
		if err := u.state.AddOrReplace(object); err != nil {
			u.errors.Append(err)
			return
		}

		// Add to changed list
		if exists {
			u.changes.AddUpdated(object)
		} else {
			u.changes.AddCreated(object)
		}
	})

	// Send
	request.Send()
}

// Delete remote object.
func (u *uow) Delete(key model.Key) {
	object, found := u.state.Get(key)
	if !found {
		return
	}

	if branch, ok := object.(*model.Branch); ok && branch.IsDefault {
		u.errors.Append(fmt.Errorf("default branch cannot be deleted"))
		return
	}
	u.delete(object)
}

func (u *uow) loadBranch(branch *model.Branch, pool *client.Pool) {
	// Load metadata for configurations
	metadataMap, metadataReq := u.configsMetadataRequest(branch, pool)

	// Load components, configs and rows
	componentsReq := pool.
		Request(u.state.api.ListComponentsRequest(branch.Id)).
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
					if objectState, err := u.addObject(config.Config); err != nil {
						u.errors.Append(err)
						continue
					} else if objectState == nil {
						// Ignored -> skip
						continue
					}

					// Rows
					for _, row := range config.Rows {
						//  Store row to state
						if _, err := u.addObject(row); err != nil {
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

func (u *uow) createOrUpdate(recipe *model.RemoteSaveRecipe, changedFields model.ChangedFields) (*client.Request, bool, error) {
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
	_, exists := u.state.Get(recipe.Object.Key())
	setMetadata := !exists || changedFields.Has("metadata")
	var setMetadataReq *client.Request
	if setMetadata {
		changedFields.Remove("metadata")
		setMetadataReq = u.state.api.AppendMetadataRequest(recipe.Object)
	}

	// Create or update
	var createOrUpdateReq *client.Request
	var err error
	if exists {
		createOrUpdateReq, err = u.updateRequest(recipe, changedFields)
	} else {
		createOrUpdateReq, err = u.createRequest(recipe)
	}
	if err != nil {
		return nil, exists, err
	}

	// Set metadata
	if setMetadataReq != nil {
		if createOrUpdateReq == nil {
			// Set metadata now because there is no change in the object.
			u.poolFor(recipe.Object.Level()).Request(setMetadataReq).Send()
		} else {
			// Set metadata if save has been successful.
			createOrUpdateReq.OnSuccess(func(response *client.Response) {
				response.WaitFor(setMetadataReq)
				response.Sender().Send(setMetadataReq) // use same pool
			})
		}
	}

	return createOrUpdateReq, exists, nil
}

func (u *uow) addObject(object model.Object) (model.Object, error) {
	// Skip ignored objects
	if u.loadFilter.IsObjectIgnored(object) {
		return nil, nil
	}

	// Invoke mapper
	internalObject := deepcopy.Copy(object).(model.Object)
	recipe := model.NewRemoteLoadRecipe(internalObject)
	if err := u.state.mapper.MapAfterRemoteLoad(recipe); err != nil {
		return nil, err
	}

	// Add object to state
	if err := u.state.AddOrReplace(internalObject); err != nil {
		return nil, err
	}

	u.changes.AddLoaded(internalObject)
	return internalObject, nil
}

func (u *uow) createRequest(recipe *model.RemoteSaveRecipe) (*client.Request, error) {
	request, err := u.state.api.CreateRequest(recipe.Object)
	if err != nil {
		return nil, err
	}

	return u.poolFor(recipe.Object.Level()).
		Request(request).
		OnError(func(response *client.Response) {
			if e, ok := response.Error().(*storageapi.Error); ok {
				if e.ErrCode == "configurationAlreadyExists" || e.ErrCode == "configurationRowAlreadyExists" {
					// Object exists -> update instead of create + clear error
					if r, err := u.updateRequest(recipe, nil); err != nil {
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

func (u *uow) updateRequest(recipe *model.RemoteSaveRecipe, changedFields model.ChangedFields) (*client.Request, error) {
	// Skip, if no field has been changed
	if changedFields.IsEmpty() {
		return nil, nil
	}

	// Create request
	request, err := u.state.api.UpdateRequest(recipe.Object, changedFields)
	if err != nil {
		return nil, err
	}

	return u.poolFor(recipe.Object.Level()).Request(request), nil
}

func (u *uow) delete(object model.Object) {
	// Branch must be deleted in blocking operation
	if branch, ok := object.(*model.Branch); ok {
		if _, err := u.state.api.DeleteBranch(branch.BranchKey); err != nil {
			u.errors.Append(err)
		}
		return
	}

	// Other types
	u.poolFor(object.Level()).
		Request(u.state.api.DeleteRequest(object.Key())).
		OnSuccess(func(response *client.Response) {
			u.state.Remove(object.Key())
			u.changes.AddDeleted(object)
		}).
		Send()
}

func (u *uow) configsMetadataRequest(branch *model.Branch, pool *client.Pool) (map[model.Key]map[string]string, *client.Request) {
	lock := &sync.Mutex{}
	out := make(map[model.Key]map[string]string)

	request := pool.
		Request(u.state.api.ListConfigMetadataRequest(branch.Id)).
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

// poolFor each level (branches, configs, rows).
func (u *uow) poolFor(level int) *client.Pool {
	if u.invoked {
		panic(fmt.Errorf(`invoked UnitOfWork cannot be reused`))
	}

	key := cast.ToString(level)
	if value, found := u.storageApiPools.Get(key); found {
		return value.(*client.Pool)
	}

	pool := u.state.api.NewPool()
	pool.SetContext(u.ctx)
	u.storageApiPools.Set(key, pool)
	return pool
}
