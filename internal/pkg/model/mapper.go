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
	ChangedFields ChangedFields
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
	Manifest       Record
	ApiObject      Object // eg. Config, original version, API representation
	InternalObject Object // eg. Config, modified version, internal representation
}

// RemoteSaveRecipe - all items related to the object, when saving to Storage API.
type RemoteSaveRecipe struct {
	ChangedFields  ChangedFields
	Manifest       Record
	InternalObject Object // eg. Config, original version, internal representation
	ApiObject      Object // eg. Config, modified version, API representation
}

// PersistRecipe contains object to persist.
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

type PathsGenerator interface {
	AddRenamed(path RenamedPath)
	RenameEnabled() bool // if true, existing paths will be renamed
}

// OnObjectPathUpdateEvent contains object with updated path.
type OnObjectPathUpdateEvent struct {
	PathsGenerator PathsGenerator
	ObjectState    ObjectState
	Renamed        bool
	OldPath        string
	NewPath        string
}

// OnObjectsRenameEvent contains old and new paths of renamed objects.
type OnObjectsRenameEvent struct {
	RenamedObjects []RenameAction
}
