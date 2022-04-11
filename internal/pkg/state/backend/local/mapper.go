package local

import (
	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// SaveMapper is intended to modify how the object will be saved in the filesystem.
// If you need a list of all saved objects, when they are already saved, use the AfterLocalOperationListener instead.
type SaveMapper interface {
	MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error
}

// LoadMapper is intended to modify/normalize the object internal representation after loading from filesystem.
// If you need a list of all loaded objects use AfterLocalOperationListener instead.
// Important: do not rely on other objects in the LocalLoadMapper, they may not be loaded yet.
// It is only guaranteed that the higher level object is loaded.
// For example on configuration load, the branch is already loaded, but other configurations may not be loaded yet.
// If you need to work with multiple objects (and relationships between them), use the AfterLocalOperationListener instead.
type LoadMapper interface {
	MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error
}

// BeforePersistMapper is intended to modify manifest record before persist.
// The Persist operation finds a new object in the filesystem and stores it in the manifest.
// Remote state does not change.
type BeforePersistMapper interface {
	MapBeforePersist(recipe *model.PersistRecipe) error
}

// FileLoadMapper is intended to modify file load process.
type FileLoadMapper interface {
	LoadLocalFile(def *filesystem.FileDef, fileType filesystem.FileType, next filesystem.LoadHandler) (filesystem.File, error)
}

// OnObjectPathUpdateListener is called when a local path has been updated.
// The renamed object is not saved yet in this step.
// You can use this listener if you need to rename some related objects.
// If you want to respond to the object rename
// after the change is saved to the filesystem, use the AfterLocalRenameListener instead.
type OnObjectPathUpdateListener interface {
	OnObjectPathUpdate(event model.OnObjectPathUpdateEvent) error
}

// AfterLocalRenameListener is called when the local.UnitOfWork finished all the work.
type AfterLocalRenameListener interface {
	AfterLocalRename(changes []model.RenameAction) error
}

// AfterLocalPersistListener is called when the persist operation is finished.
type AfterLocalPersistListener interface {
	AfterLocalPersist(persisted []model.Object) error
}

// AfterLocalOperationListener is called when the local.UnitOfWork finished all the work.
// The "changes" parameter contains all: loaded, created, update, saved, deleted objects.
type AfterLocalOperationListener interface {
	AfterLocalOperation(changes *model.Changes) error
}

// AfterOperationListener is called when the UnitOfWork finished all the work.
// The "changes" parameter contains all: loaded, persisted, created, update, (saved), renamed, deleted objects.
type AfterOperationListener interface {
	AfterOperation(changes *model.Changes) error
}

type Mappers []interface{}

// ForEach iterates over Mappers in the order in which they were defined.
func (m Mappers) ForEach(stopOnFailure bool, callback func(mapper interface{}) error) error {
	errs := errors.NewMultiError()
	for _, mapper := range m {
		if err := callback(mapper); err != nil {
			if stopOnFailure {
				return err
			}
			errs.Append(err)
		}
	}
	return errs.ErrorOrNil()
}

// ForEachReverse iterates over Mappers in the reverse order in which they were defined.
func (m Mappers) ForEachReverse(stopOnFailure bool, callback func(mapper interface{}) error) error {
	errs := errors.NewMultiError()
	l := len(m)
	for i := l - 1; i >= 0; i-- {
		if err := callback(m[i]); err != nil {
			if stopOnFailure {
				return err
			}
			errs.Append(err)
		}
	}
	return errs.ErrorOrNil()
}

// Mapper maps model.Object between internal and filesystem representations.
// Mapper is inspired by the middleware design pattern.
//
// For save method MapBeforeLocalSave mappers are called in reverse order (Mappers.ForEachReverse).
// For load and other methods, mappers are called in the order in which they were defined (Mappers.ForEach).
//
// Example:
// - LOAD: loading an object from filesystem is the FIRST step, then other mapping follows.
// - SAVE: saving an object to filesystem is the LAST step, after all mapping has been done.
type Mapper struct {
	state   *State
	mappers Mappers // implement part of the interfaces above
}

func NewMapper(state *State) *Mapper {
	return &Mapper{state: state}
}

// NewFileLoader create filesystem.FileLoader.
// File loading process is modified by mappers with LocalFileLoadMapper interface implemented.
func (m *Mapper) NewFileLoader(fs filesystem.Fs) filesystem.FileLoader {
	return fileloader.NewWithHandler(fs, m.LoadLocalFile)
}

func (m *Mapper) AddMapper(mapper ...interface{}) *Mapper {
	m.mappers = append(m.mappers, mapper...)
	return m
}

// MapBeforeLocalSave calls mappers with LocalSaveMapper interface implemented.
func (m *Mapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	return m.mappers.ForEachReverse(true, func(mapper interface{}) error {
		if mapper, ok := mapper.(SaveMapper); ok {
			if err := mapper.MapBeforeLocalSave(recipe); err != nil {
				return err
			}
		}
		return nil
	})
}

// MapAfterLocalLoad calls mappers with LocalLoadMapper interface implemented.
func (m *Mapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	return m.mappers.ForEach(true, func(mapper interface{}) error {
		if mapper, ok := mapper.(LoadMapper); ok {
			if err := mapper.MapAfterLocalLoad(recipe); err != nil {
				return err
			}
		}
		return nil
	})
}

// MapBeforePersist calls mappers with BeforePersistMapper interface implemented.
func (m *Mapper) MapBeforePersist(recipe *model.PersistRecipe) error {
	return m.mappers.ForEach(false, func(mapper interface{}) error {
		if mapper, ok := mapper.(BeforePersistMapper); ok {
			if err := mapper.MapBeforePersist(recipe); err != nil {
				return err
			}
		}
		return nil
	})
}

// LoadLocalFile calls mappers with LocalFileLoadMapper interface implemented.
func (m *Mapper) LoadLocalFile(def *filesystem.FileDef, fileType filesystem.FileType, defaultHandler filesystem.LoadHandler) (filesystem.File, error) {
	handler := defaultHandler

	// Generate handlers chain, eg.  mapper1(mapper2(mapper3(default())))
	err := m.mappers.ForEachReverse(true, func(mapper interface{}) error {
		if mapper, ok := mapper.(FileLoadMapper); ok {
			next := handler
			handler = func(def *filesystem.FileDef, fileType filesystem.FileType) (filesystem.File, error) {
				return mapper.LoadLocalFile(def, fileType, next)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Invoke handlers chain
	return handler(def, fileType)
}

// OnObjectPathUpdate calls mappers with OnObjectPathUpdateListener interface implemented.
func (m *Mapper) OnObjectPathUpdate(event model.OnObjectPathUpdateEvent) error {
	return m.mappers.ForEach(false, func(mapper interface{}) error {
		if mapper, ok := mapper.(OnObjectPathUpdateListener); ok {
			if err := mapper.OnObjectPathUpdate(event); err != nil {
				return err
			}
		}
		return nil
	})
}

// AfterLocalOperation calls mappers with AfterLocalRenameListener interface implemented.
func (m *Mapper) AfterLocalOperation(changes *model.Changes) error {
	return m.mappers.ForEach(false, func(mapper interface{}) error {
		if m, ok := mapper.(AfterLocalOperationListener); ok {
			if err := m.AfterLocalOperation(changes); err != nil {
				return err
			}
		} else if m, ok := mapper.(AfterOperationListener); ok {
			if err := m.AfterOperation(changes); err != nil {
				return err
			}
		}
		return nil
	})
}

// AfterLocalRename calls mappers with AfterLocalOperationListener interface implemented.
func (m *Mapper) AfterLocalRename(changes []model.RenameAction) error {
	return m.mappers.ForEach(false, func(mapper interface{}) error {
		if mapper, ok := mapper.(AfterLocalRenameListener); ok {
			if err := mapper.AfterLocalRename(changes); err != nil {
				return err
			}
		}
		return nil
	})
}

// AfterLocalPersist calls mappers with AfterLocalPersistListener interface implemented.
func (m *Mapper) AfterLocalPersist(persisted []model.Object) error {
	return m.mappers.ForEach(false, func(mapper interface{}) error {
		if mapper, ok := mapper.(AfterLocalPersistListener); ok {
			if err := mapper.AfterLocalPersist(persisted); err != nil {
				return err
			}
		}
		return nil
	})
}
