package remote

import (
	"context"
	"sort"

	"github.com/keboola/go-utils/pkg/deepcopy"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/keboola/keboola-sdk-go/v2/pkg/request"
	"github.com/sasha-s/go-deadlock"
	"github.com/spf13/cast"
	"golang.org/x/sync/semaphore"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Manager struct {
	state             model.ObjectStates
	localManager      *local.Manager
	keboolaProjectAPI *keboola.AuthorizedAPI
	mapper            *mapper.Mapper
}

type UnitOfWork struct {
	*Manager
	ctx               context.Context
	lock              *deadlock.Mutex
	changeDescription string                 // change description used for all modified configs and rows
	runGroups         *orderedmap.OrderedMap // separated run group for changes in branches, configs and rows
	changes           *model.RemoteChanges
	errors            errors.MultiError
	invoked           bool
	// Only one create/delete branch request can run simultaneously.
	// Operation is performed via Storage Job, which uses locks.
	// If we ran multiple requests, then only one job would run and the other jobs would wait.
	// The problem is that the lock is checked again after 30 seconds, so there is a long delay.
	branchesSem *semaphore.Weighted
}

func NewManager(localManager *local.Manager, keboolaProjectAPI *keboola.AuthorizedAPI, objects model.ObjectStates, mapper *mapper.Mapper) *Manager {
	return &Manager{
		state:             objects,
		localManager:      localManager,
		keboolaProjectAPI: keboolaProjectAPI,
		mapper:            mapper,
	}
}

func (m *Manager) Manifest() manifest.Manifest {
	return m.localManager.Manifest()
}

func (m *Manager) NewUnitOfWork(ctx context.Context, changeDescription string) *UnitOfWork {
	return &UnitOfWork{
		Manager:           m,
		ctx:               ctx,
		lock:              &deadlock.Mutex{},
		changeDescription: changeDescription,
		runGroups:         orderedmap.New(),
		changes:           model.NewRemoteChanges(),
		errors:            errors.NewMultiError(),
		branchesSem:       semaphore.NewWeighted(1),
	}
}

func (u *UnitOfWork) LoadAll(filter model.ObjectsFilter) {
	branches := make(map[model.BranchKey]*model.Branch)
	configs := make([]*model.ConfigWithRows, 0)
	configsLock := &deadlock.Mutex{}
	configsMetadata := make(map[model.ConfigKey]keboola.Metadata)
	configsMetadataLock := &deadlock.Mutex{}

	req := u.keboolaProjectAPI.
		ListBranchesRequest().
		WithOnSuccess(func(ctx context.Context, apiBranches *[]*keboola.Branch) error {
			wg := request.NewWaitGroup(ctx)
			for _, apiBranch := range *apiBranches {
				branch := model.NewBranch(apiBranch)

				// Is branch ignored?
				if filter.IsObjectIgnored(branch) {
					continue
				}

				// Add to slice
				branches[branch.BranchKey] = branch

				// Load branch metadata
				wg.Send(u.keboolaProjectAPI.
					ListBranchMetadataRequest(apiBranch.BranchKey).
					WithOnSuccess(func(_ context.Context, metadata *keboola.MetadataDetails) error {
						branch.Metadata = model.BranchMetadata(metadata.ToMap())
						return nil
					}),
				)

				// Load configs and rows
				wg.Send(u.keboolaProjectAPI.
					ListConfigsAndRowsFrom(apiBranch.BranchKey).
					WithOnSuccess(func(_ context.Context, components *[]*keboola.ComponentWithConfigs) error {
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
				wg.Send(u.keboolaProjectAPI.
					ListConfigMetadataRequest(apiBranch.ID).
					WithOnSuccess(func(_ context.Context, metadata *keboola.ConfigsMetadata) error {
						for _, item := range *metadata {
							configKey := model.ConfigKey{BranchID: item.BranchID, ComponentID: item.ComponentID, ID: item.ConfigID}
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
			errs := errors.NewMultiError()
			for key, branch := range branches {
				if err := u.loadObject(branch); err != nil {
					errs.Append(err)
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
				if err := u.loadObject(config.Config); err != nil {
					errs.Append(err)
					continue
				}
				for _, row := range config.Rows {
					if err := u.loadObject(row); err != nil {
						errs.Append(err)
					}
				}
			}
			return errs.ErrorOrNil()
		})

	// Add request
	u.runGroupFor(-1).Add(req)
}

func (u *UnitOfWork) loadObject(object model.Object) error {
	// Get object state
	objectState, found := u.state.Get(object.Key())

	// Create object state if needed
	if !found {
		// Create manifest record
		record, _, err := u.Manifest().CreateOrGetRecord(object.Key())
		if err != nil {
			return err
		}

		// Create object state
		objectState, err = u.state.CreateFrom(record)
		if err != nil {
			return err
		}
	}

	// Set remote state
	internalObject := deepcopy.Copy(object).(model.Object)
	objectState.SetRemoteState(internalObject)

	// Invoke mapper
	recipe := model.NewRemoteLoadRecipe(objectState.Manifest(), internalObject)
	if err := u.mapper.MapAfterRemoteLoad(u.ctx, recipe); err != nil {
		return err
	}

	u.changes.AddLoaded(objectState)
	return nil
}

func (u *UnitOfWork) SaveObject(objectState model.ObjectState, object model.Object, changedFields model.ChangedFields) {
	if v, ok := objectState.(*model.BranchState); ok && v.Remote == nil {
		// Branch cannot be created from the CLI
		u.errors.Append(errors.Errorf(`branch "%d" (%s) exists only locally, new branch cannot be created by CLI`, v.Local.ID, v.Local.Name))
		return
	}

	// Invoke mapper
	apiObject := deepcopy.Copy(object).(model.Object)
	recipe := model.NewRemoteSaveRecipe(objectState.Manifest(), apiObject, changedFields)
	if err := u.mapper.MapBeforeRemoteSave(u.ctx, recipe); err != nil {
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
			u.errors.Append(errors.New("default branch cannot be deleted"))
			return
		}
	}
	u.delete(objectState)
}

func (u *UnitOfWork) Invoke() error {
	if u.invoked {
		panic(errors.New(`invoked UnitOfWork cannot be reused`))
	}

	// Start and wait for all groups
	u.runGroups.SortKeys(sort.Strings)
	for _, level := range u.runGroups.Keys() {
		grp, _ := u.runGroups.Get(level)
		if err := grp.(*request.RunGroup).RunAndWait(); err != nil {
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
	if v, ok := recipe.Object.(model.ToAPIMetadata); ok && setMetadata {
		// If the object already exists, we can send the metadata request in parallel with the update.
		metadataRequestLevel := object.Level()
		if !exists {
			// If the object does not exist, we must set metadata after object creation.
			metadataRequestLevel = object.Level() + 1
		}
		changedFields.Remove("metadata")
		u.runGroupFor(metadataRequestLevel).Add(u.keboolaProjectAPI.AppendMetadataRequest(v.ToAPIObjectKey(), v.ToAPIMetadata()))
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

func (u *UnitOfWork) createRequest(objectState model.ObjectState, object model.Object, recipe *model.RemoteSaveRecipe) request.APIRequest[keboola.Object] {
	apiObject, _ := recipe.Object.(model.ToAPIObject).ToAPIObject(u.changeDescription, nil)
	request := u.keboolaProjectAPI.
		CreateRequest(apiObject).
		WithOnSuccess(func(_ context.Context, apiObject keboola.Object) error {
			// Update internal state
			object.SetObjectID(apiObject.ObjectID())
			objectState.SetRemoteState(object)
			u.changes.AddCreated(objectState)
			return nil
		}).
		WithOnError(func(ctx context.Context, err error) error {
			var storageAPIErr *keboola.StorageError
			if errors.As(err, &storageAPIErr) {
				if storageAPIErr.ErrCode == "configurationAlreadyExists" || storageAPIErr.ErrCode == "configurationRowAlreadyExists" {
					// Object exists -> update instead of create
					// This can happen if there is a disconnected "variables" configuration, and push connects it again.
					// See TestCliE2E/push/variables-add-relation
					return u.updateRequest(objectState, object, recipe, nil).SendOrErr(ctx)
				}
			}
			return err
		})

	// Limit concurrency of branch operations, see u.branchesSem comment.
	if object.Kind().IsBranch() {
		request.
			WithBefore(func(ctx context.Context) error {
				return u.branchesSem.Acquire(ctx, 1)
			}).
			WithOnComplete(func(_ context.Context, _ keboola.Object, err error) error {
				u.branchesSem.Release(1)
				return err
			})
	}

	return request
}

func (u *UnitOfWork) updateRequest(objectState model.ObjectState, object model.Object, recipe *model.RemoteSaveRecipe, changedFields model.ChangedFields) request.APIRequest[keboola.Object] {
	apiObject, apiChangedFields := recipe.Object.(model.ToAPIObject).ToAPIObject(u.changeDescription, changedFields)
	return u.keboolaProjectAPI.
		UpdateRequest(apiObject, apiChangedFields).
		WithOnSuccess(func(_ context.Context, apiObject keboola.Object) error {
			// Update internal state
			objectState.SetRemoteState(object)
			u.changes.AddUpdated(objectState)
			return nil
		})
}

func (u *UnitOfWork) delete(objectState model.ObjectState) {
	req := u.keboolaProjectAPI.
		DeleteRequest(objectState.(model.ToAPIObjectKey).ToAPIObjectKey()).
		WithOnSuccess(func(_ context.Context, _ request.NoResult) error {
			u.Manifest().Delete(objectState)
			objectState.SetRemoteState(nil)
			u.changes.AddDeleted(objectState)
			return nil
		})

	// Limit concurrency of branch operations, see u.branchesSem comment.
	if objectState.Kind().IsBranch() {
		req.
			WithBefore(func(ctx context.Context) error {
				return u.branchesSem.Acquire(ctx, 1)
			}).
			WithOnComplete(func(_ context.Context, _ request.NoResult, err error) error {
				u.branchesSem.Release(1)
				return err
			})
	}

	grp := u.runGroupFor(objectState.Level())
	grp.Add(req)
}

// runGroupFor each level (branches, configs, rows).
func (u *UnitOfWork) runGroupFor(level int) *request.RunGroup {
	if u.invoked {
		panic(errors.New(`invoked UnitOfWork cannot be reused`))
	}

	key := cast.ToString(level)
	if value, found := u.runGroups.Get(key); found {
		return value.(*request.RunGroup)
	}

	grp := request.NewRunGroup(u.ctx, u.keboolaProjectAPI.Client())
	u.runGroups.Set(key, grp)
	return grp
}
