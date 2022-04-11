package remote

import (
	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// SaveMapper is intended to modify how the object will be saved in the Storage API.
// If you need a list of all saved objects, when they are already saved, use the AfterRemoteOperationListener instead.
type SaveMapper interface {
	MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error
}

// LoadMapper is intended to modify/normalize the object internal representation after loading from the Storage API.
// If you need a list of all loaded objects use the AfterRemoteOperationListener instead.
// Important: do not rely on other objects in the RemoteLoadMapper, they may not be loaded yet.
// It is only guaranteed that the higher level object is loaded.
// For example on configuration load, the branch is already loaded, but other configurations may not be loaded yet.
// If you need to work with multiple objects (and relationships between them), use the AfterRemoteOperationListener instead.
type LoadMapper interface {
	MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error
}

// AfterRemoteOperationListener is called when the remote.UnitOfWork finished all the work.
// The "changes" parameter contains all: loaded, created, update, saved, deleted objects.
type AfterRemoteOperationListener interface {
	AfterRemoteOperation(changes *model.Changes) error
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

// MapBeforeRemoteSave calls mappers with RemoteSaveMapper interface implemented.
func (m *Mapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	return m.mappers.ForEachReverse(true, func(mapper interface{}) error {
		if mapper, ok := mapper.(SaveMapper); ok {
			if err := mapper.MapBeforeRemoteSave(recipe); err != nil {
				return err
			}
		}
		return nil
	})
}

// MapAfterRemoteLoad calls mappers with RemoteLoadMapper interface implemented.
func (m *Mapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	return m.mappers.ForEach(true, func(mapper interface{}) error {
		if mapper, ok := mapper.(LoadMapper); ok {
			if err := mapper.MapAfterRemoteLoad(recipe); err != nil {
				return err
			}
		}
		return nil
	})
}

// AfterRemoteOperation calls mappers with AfterRemoteOperationListener interface implemented.
func (m *Mapper) AfterRemoteOperation(changes *model.Changes) error {
	return m.mappers.ForEach(false, func(mapper interface{}) error {
		if m, ok := mapper.(AfterRemoteOperationListener); ok {
			if err := m.AfterRemoteOperation(changes); err != nil {
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
