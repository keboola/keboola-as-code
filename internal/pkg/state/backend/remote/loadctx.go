package remote

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// LoadContext - all items related to the object, when loading from Storage API.
type LoadContext struct {
	state       *State
	object      model.Object
	annotations map[string]interface{} // key/value pairs that can be used by to affect mappers behavior
}

func NewLoadContext(object model.Object) *LoadContext {
	return &LoadContext{
		object:      object,
		annotations: make(map[string]interface{}),
	}
}

func (c *LoadContext) State() *State {
	return c.state
}

func (c *LoadContext) Object() model.Object {
	return c.object
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
