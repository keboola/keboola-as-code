package mapper

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// LocalSaveMapper is intended to modify how the object will be saved in the filesystem.
// If you need a list of all saved objects, when they are already saved, use the AfterLocalOperationListener instead.
type LocalSaveMapper interface {
	MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error
}

// LocalLoadMapper is intended to modify/normalize the object internal representation after loading from the filesystem.
// If you need a list of all loaded objects use AfterLocalOperationListener instead.
// Important: do not rely on other objects in the LocalLoadMapper, they may not be loaded yet.
// If you need to work with multiple objects (and relationships between them), use the AfterLocalOperationListener instead.
type LocalLoadMapper interface {
	MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error
}

// RemoteSaveMapper is intended to modify how the object will be saved in the Storage API.
// If you need a list of all saved objects, when they are already saved, use the AfterRemoteOperationListener instead.
type RemoteSaveMapper interface {
	MapBeforeRemoteSave(ctx context.Context, recipe *model.RemoteSaveRecipe) error
}

// RemoteLoadMapper is intended to modify/normalize the object internal representation after loading from the Storage API.
// If you need a list of all loaded objects use the AfterRemoteOperationListener instead.
// Important: do not rely on other objects in the RemoteLoadMapper, they may not be loaded yet.
// If you need to work with multiple objects (and relationships between them), use the AfterRemoteOperationListener instead.
type RemoteLoadMapper interface {
	MapAfterRemoteLoad(ctx context.Context, recipe *model.RemoteLoadRecipe) error
}

// BeforePersistMapper is intended to modify manifest record before persist.
// The Persist operation finds a new object in the filesystem and stores it in the manifest.
// Remote state does not change.
type BeforePersistMapper interface {
	MapBeforePersist(ctx context.Context, recipe *model.PersistRecipe) error
}

// LocalFileLoadMapper is intended to modify file load process.
type LocalFileLoadMapper interface {
	LoadLocalFile(def *filesystem.FileDef, fileType filesystem.FileType, next filesystem.LoadHandler) (filesystem.File, error)
}

// OnObjectPathUpdateListener is called when a local path has been updated.
// The renamed object is not saved yet in this step.
// You can use this listener if you need to rename some related objects.
// If you want to respond to the object rename
// after the change is saved to the filesystem, use the AfterLocalOperationListener instead.
type OnObjectPathUpdateListener interface {
	OnObjectPathUpdate(event model.OnObjectPathUpdateEvent) error
}

// AfterLocalOperationListener is called when the local.UnitOfWork finished all the work.
// The "changes" parameter contains all: loaded, persisted, created, update, (saved), renamed, deleted objects.
type AfterLocalOperationListener interface {
	AfterLocalOperation(ctx context.Context, changes *model.LocalChanges) error
}

// AfterRemoteOperationListener is called when the remote.UnitOfWork finished all the work.
// The "changes" parameter contains all: loaded, created, update, (saved), deleted objects.
type AfterRemoteOperationListener interface {
	AfterRemoteOperation(ctx context.Context, changes *model.RemoteChanges) error
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

// Mapper maps model.Object between internal/filesystem/API representations.
// Mapper is inspired by the middleware design pattern.
//
// For save methods: MapBeforeLocalSave, MapBeforeRemoteSave, mappers are called in reverse order (Mappers.ForEachReverse).
// For load and other methods, mappers are called in the order in which they were defined (Mappers.ForEach).
//
// Example:
// - LOAD: loading an object from the filesystem is the FIRST step, then other mapping follows.
// - SAVE: saving an object from the filesystem is the LAST step, after all mapping has been done.
type Mapper struct {
	mappers Mappers // implement part of the interfaces above
}

func New() *Mapper {
	return &Mapper{}
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
func (m *Mapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	return m.mappers.ForEachReverse(true, func(mapper interface{}) error {
		if mapper, ok := mapper.(LocalSaveMapper); ok {
			if err := mapper.MapBeforeLocalSave(ctx, recipe); err != nil {
				return err
			}
		}
		return nil
	})
}

// MapAfterLocalLoad calls mappers with LocalLoadMapper interface implemented.
func (m *Mapper) MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	return m.mappers.ForEach(true, func(mapper interface{}) error {
		if mapper, ok := mapper.(LocalLoadMapper); ok {
			if err := mapper.MapAfterLocalLoad(ctx, recipe); err != nil {
				return err
			}
		}
		return nil
	})
}

// MapBeforeRemoteSave calls mappers with RemoteSaveMapper interface implemented.
func (m *Mapper) MapBeforeRemoteSave(ctx context.Context, recipe *model.RemoteSaveRecipe) error {
	return m.mappers.ForEachReverse(true, func(mapper interface{}) error {
		if mapper, ok := mapper.(RemoteSaveMapper); ok {
			if err := mapper.MapBeforeRemoteSave(ctx, recipe); err != nil {
				return err
			}
		}
		return nil
	})
}

// MapAfterRemoteLoad calls mappers with RemoteLoadMapper interface implemented.
func (m *Mapper) MapAfterRemoteLoad(ctx context.Context, recipe *model.RemoteLoadRecipe) error {
	return m.mappers.ForEach(true, func(mapper interface{}) error {
		if mapper, ok := mapper.(RemoteLoadMapper); ok {
			if err := mapper.MapAfterRemoteLoad(ctx, recipe); err != nil {
				return err
			}
		}
		return nil
	})
}

// MapBeforePersist calls mappers with BeforePersistMapper interface implemented.
func (m *Mapper) MapBeforePersist(ctx context.Context, recipe *model.PersistRecipe) error {
	return m.mappers.ForEach(false, func(mapper interface{}) error {
		if mapper, ok := mapper.(BeforePersistMapper); ok {
			if err := mapper.MapBeforePersist(ctx, recipe); err != nil {
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
		if mapper, ok := mapper.(LocalFileLoadMapper); ok {
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

// AfterLocalOperation calls mappers with AfterLocalOperationListener interface implemented.
func (m *Mapper) AfterLocalOperation(ctx context.Context, changes *model.LocalChanges) error {
	return m.mappers.ForEach(false, func(mapper interface{}) error {
		if mapper, ok := mapper.(AfterLocalOperationListener); ok {
			if err := mapper.AfterLocalOperation(ctx, changes); err != nil {
				return err
			}
		}
		return nil
	})
}

// AfterRemoteOperation calls mappers with AfterRemoteOperationListener interface implemented.
func (m *Mapper) AfterRemoteOperation(ctx context.Context, changes *model.RemoteChanges) error {
	return m.mappers.ForEach(false, func(mapper interface{}) error {
		if mapper, ok := mapper.(AfterRemoteOperationListener); ok {
			if err := mapper.AfterRemoteOperation(ctx, changes); err != nil {
				return err
			}
		}
		return nil
	})
}
