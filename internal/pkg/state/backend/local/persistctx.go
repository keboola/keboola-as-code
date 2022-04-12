package local

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// PersistContext contains object to persist.
type PersistContext struct {
	ctx       context.Context
	state     *State
	key       model.Key
	parentKey model.Key
	relations *model.Relations
}

func NewPersistContext(parentCtx context.Context, state *State, key, parentKey model.Key, relations *model.Relations) *PersistContext {
	return &PersistContext{
		ctx:       parentCtx,
		state:     state,
		key:       key,
		parentKey: parentKey,
		relations: relations,
	}
}

func (c *PersistContext) Ctx() context.Context {
	return c.ctx
}

func (c *PersistContext) State() *State {
	return c.state
}
func (c *PersistContext) Key() model.Key {
	return c.key
}

func (c *PersistContext) ParentKey() model.Key {
	return c.parentKey
}

func (c *PersistContext) Relations() *model.Relations {
	return c.relations
}
