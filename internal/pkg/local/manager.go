package local

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/spf13/cast"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type Manager struct {
	state    *model.State
	logger   *zap.SugaredLogger
	fs       filesystem.Fs
	manifest *manifest.Manifest
	mapper   *mapper.Mapper
}

type UnitOfWork struct {
	*Manager
	ctx             context.Context
	workers         *orderedmap.OrderedMap // separated workers for changes in branches, configs and rows
	errors          *utils.MultiError
	lock            *sync.Mutex
	skipNotFoundErr bool
	localObjects    *model.StateObjects
	changes         *model.LocalChanges
	invoked         bool
}

func NewManager(logger *zap.SugaredLogger, fs filesystem.Fs, m *manifest.Manifest, state *model.State, mapper *mapper.Mapper) *Manager {
	return &Manager{
		state:    state,
		logger:   logger,
		fs:       fs,
		manifest: m,
		mapper:   mapper,
	}
}

func (m *Manager) Manifest() *manifest.Manifest {
	return m.manifest
}

func (m *Manager) Naming() *model.Naming {
	return m.manifest.Naming
}

func (m *Manager) Fs() filesystem.Fs {
	return m.fs
}

func (m *Manager) NewUnitOfWork(ctx context.Context) *UnitOfWork {
	u := &UnitOfWork{
		Manager:      m,
		ctx:          ctx,
		workers:      orderedmap.New(),
		lock:         &sync.Mutex{},
		errors:       utils.NewMultiError(),
		localObjects: m.state.LocalObjects(),
		changes:      model.NewLocalChanges(),
	}
	return u
}

func (u *UnitOfWork) SkipNotFoundErr() {
	u.skipNotFoundErr = true
}

func (u *UnitOfWork) LoadAll(manifestContent *manifest.Content) {
	// Branches
	for _, b := range manifestContent.Branches {
		u.LoadObject(b)
	}

	// Configs
	for _, c := range manifestContent.Configs {
		u.LoadObject(c.ConfigManifest)

		// Rows
		for _, r := range c.Rows {
			u.LoadObject(r)
		}
	}
}

func (u *UnitOfWork) CreateObject(key model.Key, name string) {
	// Create object
	object, err := u.createObject(key, name)
	if err != nil {
		u.errors.Append(err)
		return
	}

	// Create manifest record
	record, _, err := u.manifest.CreateOrGetRecord(object.Key())
	if err != nil {
		u.errors.Append(err)
		return
	}

	// Set local state and manifest
	objectState, err := u.state.GetOrCreateFrom(record)
	if err != nil {
		u.errors.Append(err)
		return
	}
	objectState.SetLocalState(object)
	u.changes.AddCreated(objectState)

	// Generate local path
	if err := u.NewPathsGenerator(false).Add(objectState).Invoke(); err != nil {
		u.errors.Append(err)
		return
	}

	// Save
	u.SaveObject(objectState, object, model.ChangedFields{})
}

func (u *UnitOfWork) LoadObject(manifest model.ObjectManifest) {
	persist := !manifest.State().Persisted
	u.
		workersFor(manifest.Level()).
		AddWorker(func() error {
			// Has been parent loaded?
			if parentKey, err := manifest.Key().ParentKey(); err != nil {
				return err
			} else if parentKey != nil {
				// Has object a parent?
				if _, found := u.localObjects.Get(parentKey); !found {
					// Parent is not loaded -> skip
					manifest.State().SetInvalid()
					if parent, found := u.manifest.GetRecord(parentKey); found && parent.State().IsNotFound() {
						// Parent is not found
						manifest.State().SetNotFound()
						if !u.skipNotFoundErr {
							return fmt.Errorf(`%s "%s" not found`, manifest.Kind().Name, manifest.Path())
						}
					}
					return nil
				}
			}

			// Load object from filesystem
			object := manifest.NewEmptyObject()
			if found, err := u.Manager.loadObject(manifest, object); err != nil {
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
			if u.manifest.IsObjectIgnored(object) {
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

func (u *UnitOfWork) SaveObject(objectState model.ObjectState, object model.Object, changedFields model.ChangedFields) {
	u.
		workersFor(objectState.Level()).
		AddWorker(func() error {
			if err := u.Manager.saveObject(objectState.Manifest(), object, changedFields); err != nil {
				return err
			}
			objectState.SetLocalState(object)
			u.changes.AddSaved(objectState)
			return nil
		})
}

func (u *UnitOfWork) DeleteObject(objectState model.ObjectState, manifest model.ObjectManifest) {
	u.
		workersFor(manifest.Level()).
		AddWorker(func() error {
			if err := u.Manager.deleteObject(manifest); err != nil {
				return err
			}
			// ObjectState can be nil, if object exists only in manifest, but not in local/remote state
			if objectState != nil {
				objectState.SetLocalState(nil)
			}
			u.changes.AddDeleted(manifest)
			return nil
		})
}

func (u *UnitOfWork) Rename(actions []model.RenameAction) {
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

func (u *UnitOfWork) Invoke() error {
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

	// OnLocalChange event
	if !u.changes.Empty() {
		if err := u.mapper.OnLocalChange(u.changes); err != nil {
			u.errors.Append(err)
		}
	}

	if u.errors.Len() == 0 {
		// Delete empty directories, eg. no extractor of a type left -> dir is empty
		if err := DeleteEmptyDirectories(u.fs, u.state.TrackedPaths()); err != nil {
			u.errors.Append(err)
		}
	}

	// Update tracked paths
	if err := u.state.ReloadPathsState(); err != nil {
		u.errors.Append(err)
	}

	u.invoked = true
	return u.errors.ErrorOrNil()
}

// workersFor each level (branches, configs, rows).
func (u *UnitOfWork) workersFor(level int) *Workers {
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
