package local

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type LoadContext struct {
	ctx         context.Context
	object      model.Object           // object, eg. Config
	state       *State                 // local state
	annotations map[string]interface{} // key/value pairs that can be used by to affect mappers behavior
	basePath    model.AbsPath
	loader      filesystem.FileLoader
	loaded      *filesystem.Files
}

type fileToLoad struct {
	ctx *LoadContext
	*filesystem.FileDef
	fsLoader filesystem.FileLoader
}

func NewLoadContext(ctx context.Context, fileLoader filesystem.FileLoader, state *State, object model.Object) (*LoadContext, error) {
	basePath, err := state.GetPath(object)
	if err != nil {
		return nil, err
	}

	return &LoadContext{
		ctx:         ctx,
		object:      object,
		state:       state,
		annotations: make(map[string]interface{}),
		loader:      fileLoader,
		loaded:      filesystem.NewFiles(),
		basePath:    basePath,
	}, nil
}

func (c *LoadContext) Ctx() context.Context {
	return c.ctx
}

func (c *LoadContext) Object() model.Object {
	return c.object
}

func (c *LoadContext) State() *State {
	return c.state
}

func (c *LoadContext) Annotation(key string) (interface{}, bool) {
	v, ok := c.annotations[key]
	return v, ok
}

func (c *LoadContext) AnnotationOrNil(key string) interface{} {
	v, _ := c.annotations[key]
	return v
}

func (c *LoadContext) SetAnnotation(key string, value interface{}) *LoadContext {
	c.annotations[key] = value
	return c
}

func (c *LoadContext) BasePath() model.AbsPath {
	return c.basePath
}

func (c *LoadContext) Load(path string) *fileToLoad {
	return &fileToLoad{ctx: c, fsLoader: c.loader, FileDef: filesystem.NewFileDef(path)}
}

func (c *LoadContext) Loaded() []filesystem.File {
	return c.loaded.All()
}

func (c *LoadContext) GetOneByTag(tag string) filesystem.File {
	return c.loaded.GetOneByTag(tag)
}

func (c *LoadContext) GetByTag(tag string) []filesystem.File {
	return c.loaded.GetByTag(tag)
}

func (c *LoadContext) addLoaded(file filesystem.File) {
	if file == nil {
		panic(fmt.Errorf(`file cannot be nil`))
	}
	c.loaded.Add(file)
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

func (f *fileToLoad) ReadFile() (*filesystem.RawFile, error) {
	file, err := f.fsLoader.ReadRawFile(f.FileDef)
	if err != nil {
		return nil, err
	}
	f.ctx.addLoaded(file)
	return file, nil
}

func (f *fileToLoad) ReadJsonFieldsTo(target interface{}, tag string) (*filesystem.JsonFile, bool, error) {
	file, tagFound, err := f.fsLoader.ReadJsonFieldsTo(f.FileDef, target, tag)
	if err != nil {
		return nil, false, err
	}
	if tagFound {
		f.ctx.addLoaded(file)
	}
	return file, tagFound, nil
}

func (f *fileToLoad) ReadJsonMapTo(target interface{}, tag string) (*filesystem.JsonFile, bool, error) {
	file, tagFound, err := f.fsLoader.ReadJsonMapTo(f.FileDef, target, tag)
	if err != nil {
		return nil, false, err
	}
	if tagFound {
		f.ctx.addLoaded(file)
	}
	return file, tagFound, nil
}

func (f *fileToLoad) ReadFileContentTo(target interface{}, tag string) (*filesystem.RawFile, bool, error) {
	file, tagFound, err := f.fsLoader.ReadFileContentTo(f.FileDef, target, tag)
	if err != nil {
		return nil, false, err
	}
	if tagFound {
		f.ctx.addLoaded(file)
	}
	return file, tagFound, nil
}

func (f *fileToLoad) ReadJsonFile() (*filesystem.JsonFile, error) {
	file, err := f.fsLoader.ReadJsonFile(f.FileDef)
	if err != nil {
		return nil, err
	}
	f.ctx.addLoaded(file)
	return file, nil
}

func (f *fileToLoad) ReadJsonFileTo(target interface{}) (*filesystem.RawFile, error) {
	file, err := f.fsLoader.ReadJsonFileTo(f.FileDef, target)
	if err != nil {
		return nil, err
	}
	f.ctx.addLoaded(file)
	return file, nil
}
