package local

import (
	"context"
	"sort"
	"sync"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type Manager struct {
	logger          log.Logger
	state           model.ObjectStates
	validator       validator.Validator
	fs              filesystem.Fs
	fileLoader      filesystem.FileLoader
	manifest        manifest.Manifest
	namingGenerator *naming.Generator
	mapper          *mapper.Mapper
}

type UnitOfWork struct {
	*Manager
	ctx             context.Context
	errors          errors.MultiError
	skipNotFoundErr bool
	localObjects    model.Objects
	changes         *model.LocalChanges
	invoked         bool

	lock    *sync.Mutex
	workers *orderedmap.OrderedMap // separated workers for changes in branches, configs and rows
}

func NewManager(logger log.Logger, validator validator.Validator, fs filesystem.Fs, fileLoader filesystem.FileLoader, m manifest.Manifest, namingGenerator *naming.Generator, objects model.ObjectStates, mapper *mapper.Mapper) *Manager {
	return &Manager{
		logger:          logger,
		state:           objects,
		validator:       validator,
		fs:              fs,
		fileLoader:      fileLoader,
		manifest:        m,
		namingGenerator: namingGenerator,
		mapper:          mapper,
	}
}

func (m *Manager) Manifest() manifest.Manifest {
	return m.manifest
}

func (m *Manager) NamingGenerator() *naming.Generator {
	return m.namingGenerator
}

func (m *Manager) Fs() filesystem.Fs {
	return m.fs
}

func (m *Manager) FileLoader() filesystem.FileLoader {
	return m.fileLoader
}

func (m *Manager) NewUnitOfWork(ctx context.Context) *UnitOfWork {
	u := &UnitOfWork{
		Manager:      m,
		ctx:          ctx,
		errors:       errors.NewMultiError(),
		localObjects: m.state.LocalObjects(),
		changes:      model.NewLocalChanges(),
		lock:         &sync.Mutex{},
		workers:      orderedmap.New(),
	}
	return u
}

func (u *UnitOfWork) SkipNotFoundErr() {
	u.skipNotFoundErr = true
}

func (u *UnitOfWork) LoadAll(manifest manifest.Manifest, filter model.ObjectsFilter) {
	for _, objectManifest := range manifest.AllPersisted() {
		u.LoadObject(objectManifest, filter)
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

	// Generate local path
	if err := u.NewPathsGenerator(false).Add(objectState).Invoke(); err != nil {
		u.errors.Append(err)
		return
	}

	// Save
	u.SaveObject(objectState, object, model.ChangedFields{})
}

func (u *UnitOfWork) LoadObject(manifest model.ObjectManifest, filter model.ObjectsFilter) {
	persist := !manifest.State().IsPersisted()
	u.
		workersFor(manifest.Level()).
		AddWorker(func(ctx context.Context) error {
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
							return errors.Errorf(`%s "%s" not found`, manifest.Kind().Name, manifest.Path())
						}
					}
					return nil
				}
			}

			// Load object from filesystem
			object := manifest.NewEmptyObject()
			if found, err := u.loadObject(ctx, manifest, object); err != nil {
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
			if err := filter.AssertObjectAllowed(object); err != nil {
				switch err.Reason() {
				case model.IgnoredByAllowedBranches:
					u.logger.Warn(ctx, errors.Format(errors.NewNestedError(
						errors.Errorf("found manifest record for %s", object.Desc()),
						errors.New("it is not allowed by the manifest definition"),
						errors.New("please, remove record from the manifest and the related directory"),
						errors.New(`or modify "allowedBranches" key in the manifest`),
					), errors.FormatAsSentences()))
				case model.IgnoredByIgnoredComponents:
					u.logger.Warn(ctx, errors.Format(errors.NewNestedError(
						errors.Errorf("found manifest record for %s", object.Desc()),
						errors.New("it is not allowed by the manifest definition"),
						errors.New("please, remove record from the manifest and the related directory"),
						errors.New(`or modify "ignoredComponents" key in the manifest`),
					), errors.FormatAsSentences()))
				case model.IgnoredByAlwaysIgnoredComponents:
					u.logger.Warn(ctx, errors.Format(errors.NewNestedError(
						errors.Errorf("found manifest record for %s", object.Desc()),
						errors.New("the component cannot be configured using a definition"),
						errors.New("please, remove record from the manifest and the related directory"),
					), errors.FormatAsSentences()))
				default:
					u.logger.Warn(ctx, errors.Format(err, errors.FormatAsSentences()))
				}
				return nil
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
	isNew := !objectState.Manifest().State().IsPersisted()
	u.
		workersFor(objectState.Level()).
		AddWorker(func(ctx context.Context) error {
			if err := u.saveObject(ctx, objectState.Manifest(), object, changedFields); err != nil {
				return err
			}
			objectState.SetLocalState(object)
			if isNew {
				u.changes.AddCreated(objectState)
			} else {
				u.changes.AddUpdated(objectState)
			}
			return nil
		})
}

func (u *UnitOfWork) DeleteObject(objectState model.ObjectState, manifest model.ObjectManifest) {
	u.
		workersFor(manifest.Level()).
		AddWorker(func(ctx context.Context) error {
			if err := u.deleteObject(ctx, manifest); err != nil {
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
		AddWorker(func(ctx context.Context) error {
			if err := u.rename(ctx, actions); err != nil {
				return err
			}
			u.changes.AddRenamed(actions...)
			return nil
		})
}

func (u *UnitOfWork) Invoke() error {
	if u.invoked {
		panic(errors.New(`invoked local.UnitOfWork cannot be reused`))
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
		if err := u.mapper.AfterLocalOperation(u.ctx, u.changes); err != nil {
			u.errors.Append(err)
		}
	}

	if u.errors.Len() == 0 {
		// Delete empty directories, eg. no extractor of a type left -> dir is empty
		if err := DeleteEmptyDirectories(u.ctx, u.fs, u.state.TrackedPaths()); err != nil {
			u.errors.Append(err)
		}
	}

	// Update tracked paths
	if err := u.state.ReloadPathsState(u.ctx); err != nil {
		u.errors.Append(err)
	}

	u.invoked = true
	return u.errors.ErrorOrNil()
}

// workersFor each level (branches, configs, rows).
func (u *UnitOfWork) workersFor(level int) *Workers {
	u.lock.Lock()
	defer u.lock.Unlock()

	if u.invoked {
		panic(errors.New(`invoked local.UnitOfWork cannot be reused`))
	}

	key := cast.ToString(level)
	if value, found := u.workers.Get(key); found {
		return value.(*Workers)
	}

	workers := NewWorkers(u.ctx)
	u.workers.Set(key, workers)
	return workers
}
