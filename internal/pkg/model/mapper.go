package model

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

type MapperContext struct {
	Logger *zap.SugaredLogger
	Fs     filesystem.Fs
	Naming *Naming
	State  *State
}

// LocalLoadRecipe - all items related to the object, when loading from local fs.
type LocalLoadRecipe struct {
	Record        Record               // manifest record, eg *ConfigManifest
	Object        Object               // object, eg. Config
	Metadata      *filesystem.JsonFile // meta.json
	Configuration *filesystem.JsonFile // config.json
	Description   *filesystem.File     // description.md
	ExtraFiles    []*filesystem.File   // extra files
}

// LocalSaveRecipe - all items related to the object, when saving to local fs.
type LocalSaveRecipe struct {
	Record                             // manifest record, eg *ConfigManifest
	Object        Object               // object, eg. Config
	Metadata      *filesystem.JsonFile // meta.json
	Configuration *filesystem.JsonFile // config.json
	Description   *filesystem.File     // description.md
	ExtraFiles    []*filesystem.File   // extra files
	ToDelete      []string             // paths to delete, on save
}

// RemoteLoadRecipe - all items related to the object, when loading from Storage API.
type RemoteLoadRecipe struct {
	ApiObject      Object // eg. Config, original version, API representation
	InternalObject Object // eg. Config, modified version, internal representation
}

// RemoteSaveRecipe - all items related to the object, when saving to Storage API.
type RemoteSaveRecipe struct {
	Manifest       Record
	InternalObject Object // eg. Config, original version, internal representation
	ApiObject      Object // eg. Config, modified version, API representation
}

// PersistRecipe - TODO.
type PersistRecipe struct {
	ParentKey Key
	Manifest  Record
}

// OnObjectsLoadEvent contains new and all objects in the same state.
type OnObjectsLoadEvent struct {
	StateType  StateType     // StateTypeLocal or StateTypeRemote
	NewObjects []Object      // new objects loaded into the local / remote state
	AllObjects *StateObjects // eg. if has been loaded a new object to local state -> all objects in local state
}

// OnObjectsPersistEvent contains new persisted and all local objects.
type OnObjectsPersistEvent struct {
	PersistedObjects []Object      // new persisted objects
	AllObjects       *StateObjects // all local objects
}
