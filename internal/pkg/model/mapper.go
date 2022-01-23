package model

import (
	"fmt"
	"strings"

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

type ObjectFiles struct {
	files []*objectFile
}

type objectFile struct {
	file filesystem.FileWrapper
	tags map[string]bool
}

func (f *ObjectFiles) Add(file filesystem.FileWrapper) *objectFile {
	out := newObjectFile(file)
	f.files = append(f.files, out)
	return out
}

func (f *ObjectFiles) All() []*objectFile {
	out := make([]*objectFile, len(f.files))
	for i, file := range f.files {
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
	for _, file := range f.files {
		if file.HasTag(tag) {
			out = append(out, file)
		}
	}
	return out
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
	ObjectManifest                        // manifest record, eg *ConfigManifest
	Object         Object                 // object, eg. Config
	Files          ObjectFiles            // eg. config.json, meta.json, description.md, ...
	Annotations    map[string]interface{} // key/value pairs that can be used by to affect mappers behavior
}

// LocalSaveRecipe - all items related to the object, when saving to local fs.
type LocalSaveRecipe struct {
	ChangedFields  ChangedFields
	ObjectManifest                        // manifest record, eg *ConfigManifest
	Object         Object                 // object, eg. Config
	Files          ObjectFiles            // eg. config.json, meta.json, description.md, ...
	ToDelete       []string               // paths to delete, on save
	Annotations    map[string]interface{} // key/value pairs that can be used by to affect mappers behavior
}

// RemoteLoadRecipe - all items related to the object, when loading from Storage API.
type RemoteLoadRecipe struct {
	ObjectManifest
	Object      Object
	Annotations map[string]interface{} // key/value pairs that can be used by to affect mappers behavior
}

// RemoteSaveRecipe - all items related to the object, when saving to Storage API.
type RemoteSaveRecipe struct {
	ChangedFields ChangedFields
	ObjectManifest
	Object      Object
	Annotations map[string]interface{} // key/value pairs that can be used by to affect mappers behavior
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

func NewLocalLoadRecipe(manifest ObjectManifest, object Object) *LocalLoadRecipe {
	return &LocalLoadRecipe{
		Object:         object,
		ObjectManifest: manifest,
		Annotations:    make(map[string]interface{}),
	}
}

func NewLocalSaveRecipe(manifest ObjectManifest, object Object, changedFields ChangedFields) *LocalSaveRecipe {
	return &LocalSaveRecipe{
		ChangedFields:  changedFields,
		Object:         object,
		ObjectManifest: manifest,
		Annotations:    make(map[string]interface{}),
	}
}

func NewRemoteLoadRecipe(manifest ObjectManifest, object Object) *RemoteLoadRecipe {
	return &RemoteLoadRecipe{
		Object:         object,
		ObjectManifest: manifest,
		Annotations:    make(map[string]interface{}),
	}
}

func NewRemoteSaveRecipe(manifest ObjectManifest, object Object, changedFields ChangedFields) *RemoteSaveRecipe {
	return &RemoteSaveRecipe{
		ChangedFields:  changedFields,
		Object:         object,
		ObjectManifest: manifest,
		Annotations:    make(map[string]interface{}),
	}
}
