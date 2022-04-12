package remote

import (
	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// LoadMapper is intended to modify/normalize the object internal representation after loading from the Storage API.
// If you need a list of all loaded objects use the AfterRemoteOperationListener instead.
// Important: do not rely on other objects in the RemoteLoadMapper, they may not be loaded yet.
// It is only guaranteed that the higher level object is loaded.
// For example on configuration load, the branch is already loaded, but other configurations may not be loaded yet.
// If you need to work with multiple objects (and relationships between them), use the AfterRemoteOperationListener instead.
type LoadMapper interface {
	MapAfterRemoteLoad(ctx *LoadContext) error
}

// SaveMapper is intended to modify how the object will be saved in the Storage API.
// If you need a list of all saved objects, when they are already saved, use the AfterRemoteOperationListener instead.
type SaveMapper interface {
	MapBeforeRemoteSave(ctx *SaveContext) error
}

// AfterRemoteOperationListener is called when the remote.UnitOfWork finished all the work.
// The "changes" parameter contains all: loaded, created, update, saved, deleted objects.
type AfterRemoteOperationListener interface {
	AfterRemoteOperation(state *State, changes *model.Changes) error
}

// AfterOperationListener is called when the UnitOfWork finished all the work.
// The "changes" parameter contains all: loaded, persisted, created, update, (saved), renamed, deleted objects.
type AfterOperationListener interface {
	AfterOperation(state *State, changes *model.Changes) error
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

// Mapper maps model.Object between internal and API representations.
// Mapper is inspired by the middleware design pattern.
//
// For save method MapBeforeRemoteSave, mappers are called in reverse order (Mappers.ForEachReverse).
// For load and other methods, mappers are called in the order in which they were defined (Mappers.ForEach).
//
// Example:
// - LOAD: loading an object from Api is the FIRST step, then other mapping follows.
// - SAVE: saving an object from Api is the LAST step, after all mapping has been done.
type Mapper struct {
	state   *State
	mappers Mappers // implement part of the interfaces above
}

func NewMapper(state *State) *Mapper {
	return &Mapper{state: state}
}

func (m *Mapper) AddMapper(mapper ...interface{}) *Mapper {
	m.mappers = append(m.mappers, mapper...)
	return m
}

// MapAfterRemoteLoad calls mappers with RemoteLoadMapper interface implemented.
func (m *Mapper) MapAfterRemoteLoad(object model.Object) (*LoadContext, error) {
	ctx := NewLoadContext(object)
	err := m.mappers.ForEach(true, func(mapper interface{}) error {
		if mapper, ok := mapper.(LoadMapper); ok {
			if err := mapper.MapAfterRemoteLoad(ctx); err != nil {
				return err
			}
		}
		return nil
	})
	return ctx, err
}

// MapBeforeRemoteSave calls mappers with RemoteSaveMapper interface implemented.
func (m *Mapper) MapBeforeRemoteSave(object model.Object, changedFields model.ChangedFields) (*SaveContext, error) {
	ctx := NewSaveContext(m.state, object, changedFields)
	err := m.mappers.ForEachReverse(true, func(mapper interface{}) error {
		if mapper, ok := mapper.(SaveMapper); ok {
			if err := mapper.MapBeforeRemoteSave(ctx); err != nil {
				return err
			}
		}
		return nil
	})
	return ctx, err
}

// AfterRemoteOperation calls mappers with AfterRemoteOperationListener interface implemented.
func (m *Mapper) AfterRemoteOperation(changes *model.Changes) error {
	return m.mappers.ForEach(false, func(mapperRaw interface{}) error {
		if mapper, ok := mapperRaw.(AfterRemoteOperationListener); ok {
			if err := mapper.AfterRemoteOperation(m.state, changes); err != nil {
				return err
			}
		} else if mapper, ok := mapperRaw.(AfterOperationListener); ok {
			if err := mapper.AfterOperation(m.state, changes); err != nil {
				return err
			}
		}
		return nil
	})
}
