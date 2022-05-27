package model

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

const (
	FileTypeJson               = `json`
	FileTypeJsonNet            = `jsonnet`
	FileTypeMarkdown           = `markdown`
	FileTypeOther              = `other`
	FileKindObjectConfig       = `objectConfig`
	FileKindObjectMeta         = `objectMeta`
	FileKindObjectDescription  = `objectDescription`
	FileKindProjectDescription = `projectDescription`
	FileKindBlockMeta          = `blockMeta`
	FileKindCodeMeta           = `codeMeta`
	FileKindPhaseConfig        = `phaseConfig`
	FileKindTaskConfig         = `taskConfig`
	FileKindNativeCode         = `nativeCode`
	FileKindNativeSharedCode   = `nativeSharedCode`
	FileKindGitKeep            = `gitkeep`
)

type files = filesystem.Files

type FilesToSave struct {
	*files
}

func NewFilesToSave() *FilesToSave {
	return &FilesToSave{files: filesystem.NewFiles()}
}

type FilesLoader struct {
	fsLoader filesystem.FileLoader
	loaded   *filesystem.Files
}

type fileToLoad struct {
	*filesystem.FileDef
	loader   *FilesLoader
	fsLoader filesystem.FileLoader
}

func NewFilesLoader(fsLoader filesystem.FileLoader) *FilesLoader {
	return &FilesLoader{fsLoader: fsLoader, loaded: filesystem.NewFiles()}
}

func (l *FilesLoader) Load(path string) *fileToLoad {
	return &fileToLoad{loader: l, fsLoader: l.fsLoader, FileDef: filesystem.NewFileDef(path)}
}

func (l *FilesLoader) Loaded() []filesystem.File {
	return l.loaded.All()
}

func (l *FilesLoader) GetOneByTag(tag string) filesystem.File {
	return l.loaded.GetOneByTag(tag)
}

func (l *FilesLoader) GetByTag(tag string) []filesystem.File {
	return l.loaded.GetByTag(tag)
}

func (l *FilesLoader) ReadSubDirs(fs filesystem.Fs, root string) ([]string, error) {
	return l.fsLoader.ReadSubDirs(fs, root)
}

func (l *FilesLoader) addLoaded(file filesystem.File) {
	if file == nil {
		panic(fmt.Errorf(`file cannot be nil`))
	}
	l.loaded.Add(file)
}

func (f *fileToLoad) SetDescription(v string) *fileToLoad {
	f.FileDef.SetDescription(v)
	return f
}

func (f *fileToLoad) AddTag(tag string) *fileToLoad {
	f.FileDef.AddTag(tag)
	return f
}

func (f *fileToLoad) RemoveTag(tag string) *fileToLoad {
	f.FileDef.RemoveTag(tag)
	return f
}

func (f *fileToLoad) AddMetadata(key string, value interface{}) *fileToLoad {
	f.FileDef.AddMetadata(key, value)
	return f
}

func (f *fileToLoad) RemoveMetadata(key string) *fileToLoad {
	f.FileDef.RemoveMetadata(key)
	return f
}

func (f *fileToLoad) ReadFile() (*filesystem.RawFile, error) {
	file, err := f.fsLoader.ReadRawFile(f.FileDef)
	if err != nil {
		return nil, err
	}
	f.loader.addLoaded(file)
	return file, nil
}

func (f *fileToLoad) ReadJsonFieldsTo(target interface{}, tag string) (*filesystem.JsonFile, bool, error) {
	file, tagFound, err := f.fsLoader.ReadJsonFieldsTo(f.FileDef, target, tag)
	if err != nil {
		return nil, false, err
	}
	if tagFound {
		f.loader.addLoaded(file)
	}
	return file, tagFound, nil
}

func (f *fileToLoad) ReadJsonMapTo(target interface{}, tag string) (*filesystem.JsonFile, bool, error) {
	file, tagFound, err := f.fsLoader.ReadJsonMapTo(f.FileDef, target, tag)
	if err != nil {
		return nil, false, err
	}
	if tagFound {
		f.loader.addLoaded(file)
	}
	return file, tagFound, nil
}

func (f *fileToLoad) ReadFileContentTo(target interface{}, tag string) (*filesystem.RawFile, bool, error) {
	file, tagFound, err := f.fsLoader.ReadFileContentTo(f.FileDef, target, tag)
	if err != nil {
		return nil, false, err
	}
	if tagFound {
		f.loader.addLoaded(file)
	}
	return file, tagFound, nil
}

func (f *fileToLoad) ReadJsonFile() (*filesystem.JsonFile, error) {
	file, err := f.fsLoader.ReadJsonFile(f.FileDef)
	if err != nil {
		return nil, err
	}
	f.loader.addLoaded(file)
	return file, nil
}

func (f *fileToLoad) ReadJsonFileTo(target interface{}) (*filesystem.RawFile, error) {
	file, err := f.fsLoader.ReadJsonFileTo(f.FileDef, target)
	if err != nil {
		return nil, err
	}
	f.loader.addLoaded(file)
	return file, nil
}

// LocalLoadRecipe - all items related to the object, when loading from local fs.
type LocalLoadRecipe struct {
	ObjectManifest                        // manifest record, eg *ConfigManifest
	Object         Object                 // object, eg. Config
	Files          *FilesLoader           // eg. config.json, meta.json, description.md, ...
	Annotations    map[string]interface{} // key/value pairs that can be used by to affect mappers behavior
}

// LocalSaveRecipe - all items related to the object, when saving to local fs.
type LocalSaveRecipe struct {
	ChangedFields  ChangedFields
	ObjectManifest                        // manifest record, eg *ConfigManifest
	Object         Object                 // object, eg. Config
	Files          *FilesToSave           // eg. config.json, meta.json, description.md, ...
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

func NewLocalLoadRecipe(fsLoader filesystem.FileLoader, manifest ObjectManifest, object Object) *LocalLoadRecipe {
	return &LocalLoadRecipe{
		Object:         object,
		ObjectManifest: manifest,
		Files:          NewFilesLoader(fsLoader),
		Annotations:    make(map[string]interface{}),
	}
}

func NewLocalSaveRecipe(manifest ObjectManifest, object Object, changedFields ChangedFields) *LocalSaveRecipe {
	return &LocalSaveRecipe{
		ChangedFields:  changedFields,
		Object:         object,
		ObjectManifest: manifest,
		Files:          NewFilesToSave(),
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
