package exp

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/object"
)

type objectsReadOnly = model.ObjectsReadOnly

type State struct {
	objectsReadOnly // objects can be read directly, bud modified only by UnitOfWOrk
	objects         model.Objects
}

type Backend interface {
	Create(object model.Object, changedFields model.ChangedFields)
	Update(object model.Object, changedFields model.ChangedFields)
	Delete(key model.Key)
}

func NewState(sorter model.ObjectsSorter) *State {
	objects := object.NewCollection(sorter)
	return &State{objectsReadOnly: objects, objects: objects}
}
