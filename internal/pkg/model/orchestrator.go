package model

import (
	"fmt"

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

// UsedInOrchestratorRelation indicates that the owner config is used in an orchestration.
type UsedInOrchestratorRelation struct {
	ConfigId string
}

func (t *UsedInOrchestratorRelation) Type() RelationType {
	return UsedInOrchestratorRelType
}

func (t *UsedInOrchestratorRelation) Desc() string {
	return fmt.Sprintf(`used in orchestrator "%s"`, t.ConfigId)
}

func (t *UsedInOrchestratorRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.ConfigId)
}

func (t *UsedInOrchestratorRelation) ParentKey(_ Key) (Key, error) {
	return nil, nil
}

func (t *UsedInOrchestratorRelation) OtherSideKey(_ Key) Key {
	return nil
}

func (t *UsedInOrchestratorRelation) IsDefinedInManifest() bool {
	return false
}

func (t *UsedInOrchestratorRelation) IsDefinedInApi() bool {
	return false
}

func (t *UsedInOrchestratorRelation) NewOtherSideRelation(_ Object, _ *StateObjects) (Key, Relation, error) {
	return nil, nil, nil
}
