package local

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// SaveContext - all items related to the object, when saving to local fs.
type SaveContext struct {
	ctx           context.Context
	changedFields model.ChangedFields
	object        model.Object           // object, eg. Config
	state         *State                 // local state
	annotations   map[string]interface{} // key/value pairs that can be used by to affect mappers behavior
	basePath      model.AbsPath
	toSave        *filesystem.Files
	toDelete      []string // paths to delete, on save
}

func NewSaveContext(ctx context.Context, state *State, object model.Object, changedFields model.ChangedFields) (*SaveContext, error) {
	basePath, err := state.GetPath(object)
	if err != nil {
		return nil, err
	}

	return &SaveContext{
		ctx:           ctx,
		changedFields: changedFields,
		object:        object,
		state:         state,
		annotations:   make(map[string]interface{}),
		basePath:      basePath,
		toSave:        filesystem.NewFiles(),
	}, nil
}

func (c *SaveContext) Ctx() context.Context {
	return c.ctx
}

func (c *SaveContext) ChangedFields() model.ChangedFields {
	return c.changedFields
}

func (c *SaveContext) Object() model.Object {
	return c.object
}

func (c *SaveContext) State() *State {
	return c.state
}

func (c *SaveContext) Annotation(key string) (interface{}, bool) {
	v, ok := c.annotations[key]
	return v, ok
}

func (c *SaveContext) AnnotationOrNil(key string) interface{} {
	v, _ := c.annotations[key]
	return v
}

func (c *SaveContext) SetAnnotation(key string, value interface{}) *SaveContext {
	c.annotations[key] = value
	return c
}

func (c *SaveContext) BasePath() model.AbsPath {
	return c.basePath
}

func (c *SaveContext) ToSave() *filesystem.Files {
	return c.toSave
}

func (c *SaveContext) ToDelete() []string {
	return c.toDelete
}

func (c *SaveContext) AddToDelete(path string) *SaveContext {
	c.toDelete = append(c.toDelete, path)
	return c
}
