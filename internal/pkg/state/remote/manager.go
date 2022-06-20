package remote

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"
	"golang.org/x/sync/semaphore"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
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
	// Only one create/delete branch request can run simultaneously.
	// Operation is performed via Storage Job, which uses locks.
	// If we ran multiple requests, then only one job would run and the other jobs would wait.
	// The problem is that the lock is checked again after 30 seconds, so there is a long delay.
	branchesSem *semaphore.Weighted
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
		branchesSem:       semaphore.NewWeighted(1),
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
	}
	u.delete(objectState)
}

func (u *UnitOfWork) Invoke() error {
	if u.invoked {
		panic(fmt.Errorf(`invoked UnitOfWork cannot be reused`))
	}

	// Start and wait for all groups
	u.runGroups.SortKeys(sort.Strings)
	for _, level := range u.runGroups.Keys() {
		grp, _ := u.runGroups.Get(level)
		if err := grp.(*client.RunGroup).RunAndWait(); err != nil {
			u.errors.Append(err)
			break
		}
	}

	// AfterRemoteOperation event
	if !u.changes.Empty() {
		if err := u.mapper.AfterRemoteOperation(u.ctx, u.changes); err != nil {
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

func (u *UnitOfWork) createOrUpdate(objectState model.ObjectState, object model.Object, recipe *model.RemoteSaveRecipe, changedFields model.ChangedFields) {
	// Should metadata be set?
	exists := objectState.HasRemoteState()
	setMetadata := !exists || changedFields.Has("metadata")
	if v, ok := recipe.Object.(model.ToApiMetadata); ok && setMetadata {
		// If the object already exists, we can send the metadata request in parallel with the update.
		metadataRequestLevel := object.Level()
		if !exists {
			// If the object does not exist, we must set metadata after object creation.
			metadataRequestLevel = object.Level() + 1
		}
		changedFields.Remove("metadata")
		u.runGroupFor(metadataRequestLevel).Add(storageapi.AppendMetadataRequest(v.ToApiObjectKey(), v.ToApiMetadata()))
	}

	// Create or update
	if !exists {
		// Create
		u.runGroupFor(object.Level()).Add(u.createRequest(objectState, object, recipe))
	} else if !changedFields.IsEmpty() {
		// Update
		u.runGroupFor(object.Level()).Add(u.updateRequest(objectState, object, recipe, changedFields))
	}
}

func (u *UnitOfWork) createRequest(objectState model.ObjectState, object model.Object, recipe *model.RemoteSaveRecipe) client.APIRequest[storageapi.Object] {
	apiObject, _ := recipe.Object.(model.ToApiObject).ToApiObject(u.changeDescription, nil)
	request := storageapi.
		CreateRequest(apiObject).
		WithOnSuccess(func(_ context.Context, _ client.Sender, apiObject storageapi.Object) error {
			// Update internal state
			object.SetObjectId(apiObject.ObjectId())
			objectState.SetRemoteState(object)
			u.changes.AddCreated(objectState)
			return nil
		}).
		WithOnError(func(ctx context.Context, sender client.Sender, err error) error {
			var storageApiErr *storageapi.Error
			if errors.As(err, &storageApiErr) {
				if storageApiErr.ErrCode == "configurationAlreadyExists" || storageApiErr.ErrCode == "configurationRowAlreadyExists" {
					// Object exists -> update instead of create
					return u.updateRequest(objectState, object, recipe, nil).SendOrErr(ctx, sender)
				}
			}
			return err
		})

	// Limit concurrency of branch operations, see u.branchesSem comment.
	if object.Kind().IsBranch() {
		request.
			WithBefore(func(ctx context.Context, _ client.Sender) error {
				return u.branchesSem.Acquire(ctx, 1)
			}).
			WithOnComplete(func(_ context.Context, _ client.Sender, _ storageapi.Object, err error) error {
				u.branchesSem.Release(1)
				return err
			})
	}

	return request
}

func (u *UnitOfWork) updateRequest(objectState model.ObjectState, object model.Object, recipe *model.RemoteSaveRecipe, changedFields model.ChangedFields) client.APIRequest[storageapi.Object] {
	apiObject, apiChangedFields := recipe.Object.(model.ToApiObject).ToApiObject(u.changeDescription, changedFields)
	return storageapi.
		UpdateRequest(apiObject, apiChangedFields).
		WithOnSuccess(func(_ context.Context, _ client.Sender, apiObject storageapi.Object) error {
			// Update internal state
			objectState.SetRemoteState(object)
			u.changes.AddUpdated(objectState)
			return nil
		})
}

func (u *UnitOfWork) delete(objectState model.ObjectState) {
	request := storageapi.
		DeleteRequest(objectState.(model.ToApiObjectKey).ToApiObjectKey()).
		WithOnSuccess(func(_ context.Context, _ client.Sender, _ client.NoResult) error {
			u.Manifest().Delete(objectState)
			objectState.SetRemoteState(nil)
			u.changes.AddDeleted(objectState)
			return nil
		})

	// Limit concurrency of branch operations, see u.branchesSem comment.
	if objectState.Kind().IsBranch() {
		request.
			WithBefore(func(ctx context.Context, _ client.Sender) error {
				return u.branchesSem.Acquire(ctx, 1)
			}).
			WithOnComplete(func(_ context.Context, _ client.Sender, _ client.NoResult, err error) error {
				u.branchesSem.Release(1)
				return err
			})
	}

	grp := u.runGroupFor(objectState.Level())
	grp.Add(request)
}

// runGroupFor each level (branches, configs, rows).
func (u *UnitOfWork) runGroupFor(level int) *client.RunGroup {
	if u.invoked {
		panic(fmt.Errorf(`invoked UnitOfWork cannot be reused`))
	}

	key := cast.ToString(level)
	if value, found := u.runGroups.Get(key); found {
		return value.(*client.RunGroup)
	}

	grp := client.NewRunGroup(u.ctx, u.storageApiClient)
	u.runGroups.Set(key, grp)
	return grp
}
