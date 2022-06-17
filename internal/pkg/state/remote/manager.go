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
	storageApi   client.Sender
	mapper       *mapper.Mapper
}

type UnitOfWork struct {
	*Manager
	ctx               context.Context
	lock              *sync.Mutex
	changeDescription string                 // change description used for all modified configs and rows
	runGroups         *orderedmap.OrderedMap // separated run group for changes in branches, configs and rows
	changes           *model.RemoteChanges
	errors            *utils.MultiError
	invoked           bool
}

func NewManager(localManager *local.Manager, storageApi client.Sender, objects model.ObjectStates, mapper *mapper.Mapper) *Manager {
	return &Manager{
		state:        objects,
		localManager: localManager,
		storageApi:   storageApi,
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
		runGroups:         orderedmap.New(),
		changes:           model.NewRemoteChanges(),
		errors:            utils.NewMultiError(),
	}
}

func (u *UnitOfWork) LoadAll(filter model.ObjectsFilter) {
	branches := make(map[model.BranchKey]*model.Branch)
	configs := make([]*model.ConfigWithRows, 0)
	configsLock := &sync.Mutex{}
	configsMetadata := make(map[model.ConfigKey]storageapi.Metadata)
	configsMetadataLock := &sync.Mutex{}

	req := storageapi.
		ListBranchesRequest().
		WithOnSuccess(func(ctx context.Context, sender client.Sender, apiBranches *[]*storageapi.Branch) error {
			wg := client.NewWaitGroup(ctx, sender)
			for _, apiBranch := range *apiBranches {
				branch := model.NewBranch(apiBranch)

				// Is branch ignored?
				if filter.IsObjectIgnored(branch) {
					continue
				}

				// Add to slice
				branches[branch.BranchKey] = branch

				// Load branch metadata
				wg.Send(storageapi.
					ListBranchMetadataRequest(apiBranch.BranchKey).
					WithOnSuccess(func(_ context.Context, _ client.Sender, metadata *storageapi.MetadataDetails) error {
						branch.Metadata = model.BranchMetadata(metadata.ToMap())
						return nil
					}),
				)

				// Load configs and rows
				wg.Send(storageapi.
					ListConfigsAndRowsFrom(apiBranch.BranchKey).
					WithOnSuccess(func(_ context.Context, _ client.Sender, components *[]*storageapi.ComponentWithConfigs) error {
						// Save component, it contains all configs and rows
						for _, apiComponent := range *components {
							// Configs
							for _, apiConfig := range apiComponent.Configs {
								config := &model.ConfigWithRows{Config: model.NewConfig(apiConfig.Config)}

								// Is config ignored?
								if filter.IsObjectIgnored(config) {
									continue
								}

								// Add to slice
								configsLock.Lock()
								configs = append(configs, config)
								configsLock.Unlock()

								// Rows
								for _, apiRow := range apiConfig.Rows {
									row := model.NewConfigRow(apiRow)

									// Is row ignored?
									if filter.IsObjectIgnored(row) {
										continue
									}

									// Add to config
									config.Rows = append(config.Rows, row)
								}
							}
						}
						return nil
					}),
				)

				// Load configs metadata
				wg.Send(storageapi.
					ListConfigMetadataRequest(apiBranch.ID).
					WithOnSuccess(func(_ context.Context, _ client.Sender, metadata *storageapi.ConfigsMetadata) error {
						for _, item := range *metadata {
							configKey := model.ConfigKey{BranchId: item.BranchID, ComponentId: item.ComponentID, Id: item.ConfigID}
							configsMetadataLock.Lock()
							configsMetadata[configKey] = item.Metadata.ToMap()
							configsMetadataLock.Unlock()
						}
						return nil
					}),
				)
			}

			// Wait for sub-requests
			if err := wg.Wait(); err != nil {
				return err
			}

			// Process results
			errors := utils.NewMultiError()
			for key, branch := range branches {
				if _, err := u.loadObject(branch); err != nil {
					errors.Append(err)
					delete(branches, key)
				}
			}
			for _, config := range configs {
				// Skip config, if there is an error with branch and branch was not loaded.
				if _, found := branches[config.BranchKey()]; !found {
					continue
				}

				// Add config metadata
				if m, found := configsMetadata[config.ConfigKey]; found {
					config.Metadata = model.ConfigMetadata(m)
				} else {
					config.Metadata = make(model.ConfigMetadata)
				}
				if _, err := u.loadObject(config.Config); err != nil {
					errors.Append(err)
					continue
				}
				for _, row := range config.Rows {
					if _, err := u.loadObject(row); err != nil {
						errors.Append(err)
					}
				}
			}
			return errors.ErrorOrNil()
		})

	// Add request
	u.runGroupFor(-1).Add(req)
}

func (u *UnitOfWork) loadObject(object model.Object) (model.ObjectState, error) {
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

	// Prepare request
	u.createOrUpdate(objectState, object, recipe, changedFields)
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

func (u *UnitOfWork) createOrUpdate(objectState model.ObjectState, object model.Object, recipe *model.RemoteSaveRecipe, changedFields model.ChangedFields) {
	// Create or update
	var createUpdateRequest client.APIRequest[storageapi.Object]
	exists := objectState.HasRemoteState()
	if exists {
		// Update
		createUpdateRequest = u.updateRequest(objectState, object, recipe, changedFields)
	} else {
		createUpdateRequest = u.createRequest(objectState, object, recipe)
	}
	u.runGroupFor(object.Level()).Add(createUpdateRequest)

	// Should metadata be set?
	setMetadata := !exists || changedFields.Has("metadata")
	if v, ok := recipe.Object.(model.ToApiMetadata); ok && setMetadata {
		metadataRequest := storageapi.AppendMetadataRequest(v.ToApiObjectKey(), v.ToApiMetadata())
		// Nil means "nothing to do"
		if metadataRequest != nil {
			// If the object already exists, we can send the metadata request in parallel with the update.
			metadataRequestLevel := object.Level()
			if !exists {
				// If the object does not exist, we must set metadata after object creation.
				metadataRequestLevel = object.Level() + 1
			}
			u.runGroupFor(metadataRequestLevel).Add(metadataRequest)
		}
	}
}

func (u *UnitOfWork) createRequest(objectState model.ObjectState, object model.Object, recipe *model.RemoteSaveRecipe) client.APIRequest[storageapi.Object] {
	apiObject, _ := recipe.Object.(model.ToApiObject).ToApiObject(u.changeDescription, nil)
	return storageapi.
		CreateRequest(apiObject).
		WithOnSuccess(func(_ context.Context, _ client.Sender, apiObject storageapi.Object) error {
			// Update internal state
			object.SetObjectId(apiObject.ObjectId())
			objectState.SetRemoteState(object)
			u.changes.AddCreated(objectState)
			return nil
		}).
		WithOnError(func(ctx context.Context, sender client.Sender, err error) error {
			if e, ok := err.(*storageapi.Error); ok {
				if e.ErrCode == "configurationAlreadyExists" || e.ErrCode == "configurationRowAlreadyExists" {
					// Object exists -> update instead of create
					return u.updateRequest(objectState, object, recipe, nil).SendOrErr(ctx, sender)
				}
			}
			return err
		})
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
	grp := u.runGroupFor(objectState.Level())
	grp.Add(storageapi.
		DeleteRequest(objectState.(model.ToApiObjectKey).ToApiObjectKey()).
		WithOnSuccess(func(_ context.Context, _ client.Sender, _ client.NoResult) error {
			u.Manifest().Delete(objectState)
			objectState.SetRemoteState(nil)
			u.changes.AddDeleted(objectState)
			return nil
		}),
	)
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

	grp := client.NewRunGroup(u.ctx, u.storageApi)
	u.runGroups.Set(key, grp)
	return grp
}
