package model

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

const (
	FileTypeJson              = `json`
	FileTypeJsonNet           = `jsonnet`
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

type FilesToSave struct {
	*objectFiles
}

func NewFilesToSave() *FilesToSave {
	return &FilesToSave{objectFiles: newObjectFiles()}
}

type FilesLoader struct {
	fsLoader filesystem.FileLoader
	loaded   *objectFiles
}

type fileToLoad struct {
	loader   *FilesLoader
	fsLoader filesystem.FileLoader
	path     string
	desc     string
	tags     map[string]bool
}

func NewFilesLoader(fsLoader filesystem.FileLoader) *FilesLoader {
	return &FilesLoader{fsLoader: fsLoader, loaded: newObjectFiles()}
}

func (l *FilesLoader) Load(path string) *fileToLoad {
	return &fileToLoad{loader: l, fsLoader: l.fsLoader, path: path, tags: make(map[string]bool)}
}

func (l *FilesLoader) Loaded() []*objectFile {
	return l.loaded.All()
}

func (l *FilesLoader) GetOneByTag(tag string) *objectFile {
	return l.loaded.GetOneByTag(tag)
}

func (l *FilesLoader) GetByTag(tag string) []*objectFile {
	return l.loaded.GetByTag(tag)
}

func (l *FilesLoader) addLoaded(def *fileToLoad, file filesystem.FileWrapper) {
	if file == nil {
		panic(fmt.Errorf(`file cannot be nil`))
	}
	l.loaded.Add(file).AddTag(def.Tags()...)
}

func (f *fileToLoad) SetDescription(v string) *fileToLoad {
	f.desc = v
	return f
}

func (f *fileToLoad) Tags() []string {
	out := make([]string, len(f.tags))
	i := 0
	for tag := range f.tags {
		out[i] = tag
		i++
	}
	return out
}

func (f *fileToLoad) HasTag(tag string) bool {
	return f.tags[tag]
}

func (f *fileToLoad) AddTag(tags ...string) *fileToLoad {
	for _, tag := range tags {
		f.tags[tag] = true
	}
	return f
}

func (f *fileToLoad) RemoveTag(tags ...string) *fileToLoad {
	for _, tag := range tags {
		delete(f.tags, tag)
	}
	return f
}

func (f *fileToLoad) ReadFile() (*filesystem.File, error) {
	file, err := f.fsLoader.ReadFile(f.path, f.desc)
	if err != nil {
		return nil, err
	}
	f.loader.addLoaded(f, file)
	return file, nil
}

func (f *fileToLoad) ReadJsonFieldsTo(target interface{}, tag string) (*filesystem.JsonFile, bool, error) {
	file, tagFound, err := f.fsLoader.ReadJsonFieldsTo(f.path, f.desc, target, tag)
	if err != nil {
		return nil, false, err
	}
	if tagFound {
		f.loader.addLoaded(f, file)
	}
	return file, tagFound, nil
}

func (f *fileToLoad) ReadJsonMapTo(target interface{}, tag string) (*filesystem.JsonFile, bool, error) {
	file, tagFound, err := f.fsLoader.ReadJsonMapTo(f.path, f.desc, target, tag)
	if err != nil {
		return nil, false, err
	}
	if tagFound {
		f.loader.addLoaded(f, file)
	}
	return file, tagFound, nil
}

func (f *fileToLoad) ReadFileContentTo(target interface{}, tag string) (*filesystem.File, bool, error) {
	file, tagFound, err := f.fsLoader.ReadFileContentTo(f.path, f.desc, target, tag)
	if err != nil {
		return nil, false, err
	}
	if tagFound {
		f.loader.addLoaded(f, file)
	}
	return file, tagFound, nil
}

func (f *fileToLoad) ReadJsonFile() (*filesystem.JsonFile, error) {
	file, err := f.fsLoader.ReadJsonFile(f.path, f.desc)
	if err != nil {
		return nil, err
	}
	f.loader.addLoaded(f, file)
	return file, nil
}

func (f *fileToLoad) ReadJsonFileTo(target interface{}) (*filesystem.File, error) {
	file, err := f.fsLoader.ReadJsonFileTo(f.path, f.desc, target)
	if err != nil {
		return nil, err
	}
	f.loader.addLoaded(f, file)
	return file, nil
}

type objectFiles struct {
	files []*objectFile
}

func newObjectFiles() *objectFiles {
	return &objectFiles{}
}

func (f *objectFiles) All() []*objectFile {
	out := make([]*objectFile, len(f.files))
	copy(out, f.files)
	return out
}

func (f *objectFiles) Add(file filesystem.FileWrapper) *objectFile {
	out := newObjectFile(file)
	f.files = append(f.files, out)
	return out
}

func (f *objectFiles) GetOneByTag(tag string) *objectFile {
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

func (f *objectFiles) GetByTag(tag string) []*objectFile {
	var out []*objectFile
	for _, file := range f.files {
		if file.HasTag(tag) {
			out = append(out, file)
		}
	}
	return out
}

type objectFile struct {
	file filesystem.FileWrapper
	tags map[string]bool
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

func (f *objectFile) AddTag(tags ...string) *objectFile {
	for _, tag := range tags {
		f.tags[tag] = true
	}
	return f
}

func (f *objectFile) RemoveTag(tags ...string) *objectFile {
	for _, tag := range tags {
		delete(f.tags, tag)
	}
	return f
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
