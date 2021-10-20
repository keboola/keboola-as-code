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
	Record        Record               // manifest record, eg *ConfigManifest
	Object        Object               // object, eg. Config
	Metadata      *filesystem.JsonFile // meta.json
	Configuration *filesystem.JsonFile // config.json
	Description   *filesystem.File     // description.md
	ExtraFiles    []*filesystem.File   // extra files
	ToDelete      []string             // paths to delete, on save
}

// RemoteLoadRecipe - all items related to the object, when loading from Storage API.
type RemoteLoadRecipe struct{}

// RemoteSaveRecipe - all items related to the object, when saving to Storage API.
type RemoteSaveRecipe struct{}
