package remote

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// SaveContext - all items related to the object, when saving to Storage API.
type SaveContext struct {
	state         *State
	object        model.Object
	changedFields model.ChangedFields
	annotations   map[string]interface{} // key/value pairs that can be used by to affect mappers behavior
}

func NewSaveContext(state *State, object model.Object, changedFields model.ChangedFields) *SaveContext {
	return &SaveContext{
		state:         state,
		object:        object,
		changedFields: changedFields,
		annotations:   make(map[string]interface{}),
	}
}

func (c *SaveContext) State() *State {
	return c.state
}

func (c *SaveContext) Object() model.Object {
	return c.object
}

func (c *SaveContext) ChangedFields() model.ChangedFields {
	return c.changedFields
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
