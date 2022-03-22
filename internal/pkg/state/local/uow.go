package local

import (
	"context"
	"fmt"
	"sort"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// UnitOfWork executed on local.State in parallel when Invoke is called.
type UnitOfWork interface {
	Invoke() error
	SkipNotFoundError()
	LoadAll(manifest manifest.Manifest)
	Load(manifest model.ObjectManifest)
	Save(object model.Object, changedFields model.ChangedFields)
	Delete(key model.Key)
	Rename(actions []model.RenameAction)
}

type _state = State

// uow implements UnitOfWork interface.
type uow struct {
	*_state
	// Inputs
	ctx        context.Context
	loadFilter model.ObjectsFilter
	// Internals
	invoked         bool
	skipNotFoundErr bool
	workers         *orderedmap.OrderedMap // separated workers for changes in branches, configs and rows
	changes         *model.LocalChanges
	errors          *utils.MultiError
}

func newUnitOfWork(state *State, ctx context.Context, loadFilter model.ObjectsFilter) UnitOfWork {
	return &uow{
		_state:     state,
		ctx:        ctx,
		loadFilter: loadFilter,
	}
}

func (u *uow) Invoke() error {
	if u.invoked {
		panic(fmt.Errorf(`invoked local.UnitOfWork cannot be reused`))
	}

	// Start and wait for all workers
	u.workers.SortKeys(sort.Strings)
	for _, level := range u.workers.Keys() {
		worker, _ := u.workers.Get(level)
		if err := worker.(*Workers).StartAndWait(); err != nil {
			u.errors.Append(err)
		}
	}

	// AfterLocalOperation event
	if !u.changes.Empty() {
		if err := u.mapper.AfterLocalOperation(u.changes); err != nil {
			u.errors.Append(err)
		}
	}

	if u.errors.Len() == 0 {
		// Delete empty directories, eg. no extractor of a type left -> dir is empty
		if err := u.manager.DeleteEmptyDirectories(); err != nil {
			u.errors.Append(err)
		}
	}

	// Update tracked paths
	if err := u.ReloadPathsState(); err != nil {
		u.errors.Append(err)
	}

	u.invoked = true
	return u.errors.ErrorOrNil()
}

func (u *uow) SkipNotFoundError() {
	u.skipNotFoundErr = true
}

func (u *uow) LoadAll(manifest manifest.Manifest) {
	for _, objectManifest := range manifest.AllPersisted() {
		u.Load(objectManifest)
	}
}

func (u *uow) Load(manifest model.ObjectManifest) {
	persist := !manifest.State().IsPersisted()
	u.
		workersFor(manifest.Level()).
		AddWorker(func() error {
			// Has been parent loaded?
			if parentKey, err := manifest.Key().ParentKey(); err != nil {
				return err
			} else if parentKey != nil {
				// Has object a parent?
				if _, found := u.Get(parentKey); !found {
					// Parent is not loaded -> skip
					manifest.State().SetInvalid()
					if parent, found := u.manifest.GetRecord(parentKey); found && parent.State().IsNotFound() {
						// Parent is not found
						manifest.State().SetNotFound()
						if !u.skipNotFoundErr {
							return fmt.Errorf(`%s "%s" not found`, manifest.Kind().Name, manifest.String())
						}
					}
					return nil
				}
			}

			// Load object from filesystem
			object := manifest.NewEmptyObject()
			if found, err := u.manager.LoadObject(u.ctx, manifest, object); err != nil {
				manifest.State().SetInvalid()
				if !found {
					manifest.State().SetNotFound()
				}
				if found || !u.skipNotFoundErr {
					return err
				}
				return nil
			}

			// Validate, object must be allowed
			if u.loadFilter.IsObjectIgnored(object) {
				return fmt.Errorf(
					`found manifest record for %s "%s", but it is not allowed by the manifest definition`,
					object.Kind().Name,
					object.ObjectId(),
				)
			}

			// Get or create object state
			objectState, err := u.state.GetOrCreateFrom(manifest)
			if err != nil {
				return err
			}

			// Set local state
			objectState.SetLocalState(object)

			if persist {
				u.changes.AddPersisted(objectState)
			}
			u.changes.AddLoaded(objectState)
			return nil
		})
}

func (u *uow) Create(key model.Key, name string) model.Object {
	// Create object
	object, err := u.createObject(key, name)
	if err != nil {
		u.errors.Append(err)
		return
	}

	// Save
	u.Save(object, model.ChangedFields{})
	return object
}

func (u *uow) Save(object model.Object, changedFields model.ChangedFields) {
	_, exists := u.Get(object.Key())
	u.
		workersFor(object.Level()).
		AddWorker(func() error {
			// Save
			if err := u.Manager.saveObject(u.ctx, objectState.Manifest(), object, changedFields); err != nil {
				return err
			}

			// Add to state
			if err := u.AddOrReplace(object); err != nil {
				return err
			}

			// Add to changed list
			if exists {
				u.changes.AddUpdated(object)
			} else {
				u.changes.AddCreated(object)
			}
			return nil
		})
}

func (u *uow) Delete(object model.Key) {
	u.
		workersFor(object.Level()).
		AddWorker(func() error {
			if err := u.Manager.deleteObject(manifest); err != nil {
				return err
			}
			u.Remove(object)
			u.changes.AddDeleted(object)
			return nil
		})
}

func (u *uow) Rename(actions []model.RenameAction) {
	u.
		workersFor(1000). // rename at the end
		AddWorker(func() error {
			if err := u.rename(actions); err != nil {
				return err
			}
			u.changes.AddRenamed(actions...)
			return nil
		})
}

// workersFor each level (branches, configs, rows).
func (u *uow) workersFor(level int) *Workers {
	if u.invoked {
		panic(fmt.Errorf(`invoked local.UnitOfWork cannot be reused`))
	}

	key := cast.ToString(level)
	if value, found := u.workers.Get(key); found {
		return value.(*Workers)
	}

	workers := NewWorkers(u.ctx)
	u.workers.Set(key, workers)
	return workers
}
