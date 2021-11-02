package mapper

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// LocalSaveMapper to modify how the object will be saved in the filesystem.
type LocalSaveMapper interface {
	MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error
}

// LocalLoadMapper to modify/normalize the object internal representation after loading from the filesystem.
// Note: do not rely on other objects, they may not be loaded yet, see OnObjectsLoadListener.
type LocalLoadMapper interface {
	MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error
}

// RemoteSaveMapper to modify how the object will be saved in the Storage API.
type RemoteSaveMapper interface {
	MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error
}

// RemoteLoadMapper to modify/normalize the object internal representation after loading from the Storage API.
// Note: do not rely on other objects, they may not be loaded yet, see OnObjectsLoadListener.
type RemoteLoadMapper interface {
	MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error
}

// OnObjectsLoadListener is called when all new objects are loaded in local/remote state.
type OnObjectsLoadListener interface {
	OnObjectsLoad(event model.OnObjectsLoadEvent) error
}

// Mapper maps Objects between internal/filesystem/API representations.
type Mapper struct {
	context model.MapperContext
	mappers []interface{} // implement part of the interfaces above
}

func New(context model.MapperContext) *Mapper {
	return &Mapper{context: context}
}

func (m *Mapper) AddMapper(mapper ...interface{}) *Mapper {
	m.mappers = append(m.mappers, mapper...)
	return m
}

func (m *Mapper) Context() model.MapperContext {
	return m.context
}

func (m *Mapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	for _, mapper := range m.mappers {
		if mapper, ok := mapper.(LocalSaveMapper); ok {
			if err := mapper.MapBeforeLocalSave(recipe); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Mapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	for _, mapper := range m.mappers {
		if mapper, ok := mapper.(LocalLoadMapper); ok {
			if err := mapper.MapAfterLocalLoad(recipe); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Mapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	for _, mapper := range m.mappers {
		if mapper, ok := mapper.(RemoteSaveMapper); ok {
			if err := mapper.MapBeforeRemoteSave(recipe); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Mapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	for _, mapper := range m.mappers {
		if mapper, ok := mapper.(RemoteLoadMapper); ok {
			if err := mapper.MapAfterRemoteLoad(recipe); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Mapper) OnObjectsLoaded(stateType model.StateType, newObjects []model.Object) error {
	errors := utils.NewMultiError()
	event := model.OnObjectsLoadEvent{
		StateType:  stateType,
		NewObjects: newObjects,
		AllObjects: m.context.State.StateObjects(stateType),
	}
	for _, mapper := range m.mappers {
		if mapper, ok := mapper.(OnObjectsLoadListener); ok {
			if err := mapper.OnObjectsLoad(event); err != nil {
				errors.Append(err)
			}
		}
	}

	return errors.ErrorOrNil()
}
