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

// BeforePersistMapper to modify manifest record before persist.
type BeforePersistMapper interface {
	MapBeforePersist(recipe *model.PersistRecipe) error
}

// OnObjectsLoadListener is called when all new objects are loaded in local/remote state.
type OnObjectsLoadListener interface {
	OnObjectsLoad(event model.OnObjectsLoadEvent) error
}

// OnObjectsPersistListener is called when all new objects are persisted.
type OnObjectsPersistListener interface {
	OnObjectsPersist(event model.OnObjectsPersistEvent) error
}

// OnObjectPathUpdateListener is called when a local path has been updated.
type OnObjectPathUpdateListener interface {
	OnObjectPathUpdate(event model.OnObjectPathUpdateEvent) error
}

// OnObjectsRenameListener is called when some object paths have been changed.
type OnObjectsRenameListener interface {
	OnObjectsRename(event model.OnObjectsRenameEvent) error
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

func (m *Mapper) MapBeforePersist(recipe *model.PersistRecipe) error {
	for _, mapper := range m.mappers {
		if mapper, ok := mapper.(BeforePersistMapper); ok {
			if err := mapper.MapBeforePersist(recipe); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *Mapper) OnObjectsLoad(stateType model.StateType, newObjects []model.Object) error {
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

func (m *Mapper) OnObjectsPersist(persistedObjects []model.Object) error {
	errors := utils.NewMultiError()
	event := model.OnObjectsPersistEvent{
		PersistedObjects: persistedObjects,
		AllObjects:       m.context.State.LocalObjects(),
	}

	for _, mapper := range m.mappers {
		if mapper, ok := mapper.(OnObjectsPersistListener); ok {
			if err := mapper.OnObjectsPersist(event); err != nil {
				errors.Append(err)
			}
		}
	}

	return errors.ErrorOrNil()
}

func (m *Mapper) OnObjectPathUpdate(event model.OnObjectPathUpdateEvent) error {
	errors := utils.NewMultiError()
	for _, mapper := range m.mappers {
		if mapper, ok := mapper.(OnObjectPathUpdateListener); ok {
			if err := mapper.OnObjectPathUpdate(event); err != nil {
				errors.Append(err)
			}
		}
	}

	return errors.ErrorOrNil()
}

func (m *Mapper) OnObjectsRename(renamedObjects []model.RenameAction) error {
	errors := utils.NewMultiError()
	event := model.OnObjectsRenameEvent{
		RenamedObjects: renamedObjects,
	}

	for _, mapper := range m.mappers {
		if mapper, ok := mapper.(OnObjectsRenameListener); ok {
			if err := mapper.OnObjectsRename(event); err != nil {
				errors.Append(err)
			}
		}
	}

	return errors.ErrorOrNil()
}
