package mapper

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// LocalSaveMapper to modify how the object will be saved in the filesystem.
type LocalSaveMapper interface {
	MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error
}

// LocalLoadMapper to modify/normalize the object internal representation after loading from the filesystem.
// Note: do not rely on other objects, they may not be loaded yet, see OnLoadListener.
type LocalLoadMapper interface {
	MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error
}

// RemoteSaveMapper to modify how the object will be saved in the Storage API.
type RemoteSaveMapper interface {
	MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error
}

// RemoteLoadMapper to modify/normalize the object internal representation after loading from the Storage API.
// Note: do not rely on other objects, they may not be loaded yet, see OnLoadListener.
type RemoteLoadMapper interface {
	MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error
}

// OnLoadListener is called when a new object is loaded
// It is called when all new objects from the operation are loaded.
type OnLoadListener interface {
	OnLoad(event model.OnObjectLoadEvent) error
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

func (m *Mapper) OnLoad(event model.OnObjectLoadEvent) error {
	for _, mapper := range m.mappers {
		if mapper, ok := mapper.(OnLoadListener); ok {
			if err := mapper.OnLoad(event); err != nil {
				return err
			}
		}
	}

	return nil
}
