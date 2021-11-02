package local

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/iancoleman/orderedmap"
	"github.com/spf13/cast"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
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
	errors          *utils.Error
	lock            *sync.Mutex
	skipNotFoundErr bool
	newObjectStates []model.ObjectState
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
		Manager: m,
		ctx:     ctx,
		workers: utils.NewOrderedMap(),
		lock:    &sync.Mutex{},
		errors:  utils.NewMultiError(),
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
	u.addNewObjectState(objectState)

	// Generate local path
	if err := u.NewPathsGenerator(false).Update(objectState); err != nil {
		u.errors.Append(err)
		return
	}

	// Save
	u.SaveObject(objectState, object)
}

func (u *UnitOfWork) LoadObject(record model.Record) {
	u.
		workersFor(record.Level()).
		AddWorker(func() error {
			object := record.NewEmptyObject()
			if found, err := u.Manager.loadObject(record, object); err != nil {
				record.State().SetInvalid()
				if !found {
					record.State().SetNotFound()
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
			objectState, err := u.state.GetOrCreateFrom(record)
			if err != nil {
				return err
			}

			// Update tracked paths
			u.state.TrackRecord(record)

			// Set local state
			objectState.SetLocalState(object)

			u.addNewObjectState(objectState)
			return nil
		})
}

func (u *UnitOfWork) SaveObject(objectState model.ObjectState, object model.Object) {
	u.
		workersFor(objectState.Level()).
		AddWorker(func() error {
			if err := u.Manager.saveObject(objectState.Manifest(), object); err != nil {
				return err
			}
			objectState.SetLocalState(object)
			return nil
		})
}

func (u *UnitOfWork) DeleteObject(objectState model.ObjectState, record model.Record) {
	u.
		workersFor(record.Level()).
		AddWorker(func() error {
			if err := u.Manager.deleteObject(record); err != nil {
				return err
			}
			// ObjectState can be nil, if object exists only in manifest, but not in local/remote state
			if objectState != nil {
				objectState.SetLocalState(nil)
			}
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

	// OnObjectsLoad event
	if err := u.mapper.OnObjectsLoaded(model.StateTypeLocal, u.newObjects()); err != nil {
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

func (u *UnitOfWork) addNewObjectState(objectState model.ObjectState) {
	u.lock.Lock()
	defer u.lock.Unlock()
	u.newObjectStates = append(u.newObjectStates, objectState)
}

func (u *UnitOfWork) newObjects() []model.Object {
	var newObjects []model.Object
	for _, objectState := range u.newObjectStates {
		newObjects = append(newObjects, objectState.LocalState())
	}
	return newObjects
}
