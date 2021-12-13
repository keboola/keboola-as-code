package model

import (
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

const (
	FileTypeJson              = `json`
	FileTypeMarkdown          = `markdown`
	FileTypeOther             = `other`
	FileKindObjectConfig      = `objectConfig`
	FileKindObjectMeta        = `objectMeta`
	FileKindObjectDescription = `objectDescription`
	FileKindBlockMeta         = `blockMeta`
	FileKindCodeMeta          = `codeMeta`
	FileKindPhaseConfig       = `phaseConfig`
	FileKindTaskConfig        = `taskConfig`
	FileKindNativeCode        = `nativeCode`
	FileKindNativeSharedCode  = `nativeSharedCode`
	FileKindGitKeep           = `gitkeep`
)

type MapperContext struct {
	Logger *zap.SugaredLogger
	Fs     filesystem.Fs
	Naming *Naming
	State  *State
}

type ObjectFiles struct {
	Files []*objectFile
}

type objectFile struct {
	file filesystem.FileWrapper
	tags map[string]bool
}

func (f *ObjectFiles) Add(file filesystem.FileWrapper) *objectFile {
	out := newObjectFile(file)
	f.Files = append(f.Files, out)
	return out
}

func (f *ObjectFiles) All() []*objectFile {
	out := make([]*objectFile, len(f.Files))
	for i, file := range f.Files {
		out[i] = file
	}
	return out
}

func (f *ObjectFiles) GetOneByTag(tag string) *objectFile {
	files := f.GetByTag(tag)
	if len(files) == 1 {
		return files[0]
	} else if len(files) > 1 {
		var paths []string
		for _, file := range files {
			paths = append(paths, file.Path())
		}
		panic(fmt.Errorf(`found multiple files with tag "%s": "%s"`, tag, strings.Join(paths, `", "`)))
	}
	return nil
}

func (f *ObjectFiles) GetByTag(tag string) []*objectFile {
	var out []*objectFile
	for _, file := range f.Files {
		if file.HasTag(tag) {
			out = append(out, file)
		}
	}
	return out
}

func (f *ObjectFiles) ObjectConfigFile() (*filesystem.JsonFile, error) {
	raw := f.GetOneByTag(FileKindObjectConfig)
	if raw == nil {
		return nil, fmt.Errorf(`missing config file`)
	}
	file, ok := raw.File().(*filesystem.JsonFile)
	if !ok {
		return nil, fmt.Errorf(`expected JsonFile, found "%T"`, raw)
	}
	return file, nil
}

func (f *ObjectFiles) ObjectMetaFile() (*filesystem.JsonFile, error) {
	raw := f.GetOneByTag(FileKindObjectMeta)
	if raw == nil {
		return nil, fmt.Errorf(`missing config file`)
	}
	file, ok := raw.File().(*filesystem.JsonFile)
	if !ok {
		return nil, fmt.Errorf(`expected JsonFile, found "%T"`, raw)
	}
	return file, nil
}

func newObjectFile(file filesystem.FileWrapper) *objectFile {
	return &objectFile{
		file: file,
		tags: make(map[string]bool),
	}
}

func (f *objectFile) Description() string {
	return f.file.GetDescription()
}

func (f *objectFile) Path() string {
	return f.file.GetPath()
}

func (f *objectFile) File() filesystem.FileWrapper {
	return f.file
}

func (f *objectFile) ToFile() (*filesystem.File, error) {
	return f.file.ToFile()
}

func (f *objectFile) SetFile(file filesystem.FileWrapper) *objectFile {
	f.file = file
	return f
}

func (f *objectFile) HasTag(tag string) bool {
	return f.tags[tag]
}

func (f *objectFile) AddTag(tag string) *objectFile {
	f.tags[tag] = true
	return f
}

func (f *objectFile) DeleteTag(tag string) *objectFile {
	delete(f.tags, tag)
	return f
}

// LocalLoadRecipe - all items related to the object, when loading from local fs.
type LocalLoadRecipe struct {
	ObjectManifest             // manifest record, eg *ConfigManifest
	Object         Object      // object, eg. Config
	Files          ObjectFiles // eg. config.json, meta.json, description.md, ...
}

// LocalSaveRecipe - all items related to the object, when saving to local fs.
type LocalSaveRecipe struct {
	ChangedFields  ChangedFields
	ObjectManifest             // manifest record, eg *ConfigManifest
	Object         Object      // object, eg. Config
	Files          ObjectFiles // eg. config.json, meta.json, description.md, ...
	ToDelete       []string    // paths to delete, on save
}

// RemoteLoadRecipe - all items related to the object, when loading from Storage API.
type RemoteLoadRecipe struct {
	ObjectManifest
	ApiObject      Object // eg. Config, original version, API representation
	InternalObject Object // eg. Config, modified version, internal representation
}

// RemoteSaveRecipe - all items related to the object, when saving to Storage API.
type RemoteSaveRecipe struct {
	ChangedFields  ChangedFields
	ObjectManifest        // manifest record, eg *ConfigManifest
	InternalObject Object // eg. Config, original version, internal representation
	ApiObject      Object // eg. Config, modified version, API representation
}

// PersistRecipe contains object to persist.
type PersistRecipe struct {
	ParentKey Key
	Manifest  ObjectManifest
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
