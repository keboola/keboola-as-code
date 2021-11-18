package model

import (
	"github.com/iancoleman/orderedmap"
)

type Orchestration struct {
	Phases []*Phase
}

type Phase struct {
	PhaseKey
	PathInProject
	DependsOn []PhaseKey
	Tasks     []*Task                `validate:"dive"`
	Name      string                 `validate:"required"`
	Content   *orderedmap.OrderedMap `validate:"required"`
}

type Task struct {
	TaskKey
	PathInProject `validate:"dive"`
	Name          string                 `validate:"required"`
	ComponentId   string                 `validate:"required"`
	ConfigId      string                 `validate:"required"`
	Content       *orderedmap.OrderedMap `validate:"dive"`
}

func (v *Orchestration) Clone() *Orchestration {
	if v == nil {
		return nil
	}
	clone := *v
	return &clone
}
