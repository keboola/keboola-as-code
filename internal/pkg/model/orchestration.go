package model

import (
	"fmt"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

const (
	PhaseKind = "phase"
	TaskKind  = "task"
	PhaseAbbr = "p"
	TaskAbbr  = "t"
)

type PhaseKey struct {
	BranchId    BranchId    `json:"-" validate:"required_in_project" `
	ComponentId ComponentId `json:"-" validate:"required" `
	ConfigId    ConfigId    `json:"-" validate:"required" `
	Index       int         `json:"-" validate:"min=0" `
}

type TaskKey struct {
	PhaseKey `json:"-" validate:"dive" `
	Index    int `json:"-" validate:"min=0" `
}

type Orchestration struct {
	Phases []*Phase
}

type Phase struct {
	PhaseKey
	DependsOn []PhaseKey
	Tasks     []*Task                `validate:"dive"`
	Name      string                 `validate:"required"`
	Content   *orderedmap.OrderedMap `validate:"required"`
}

type Task struct {
	TaskKey
	Name        string                 `validate:"required"`
	ComponentId ComponentId            `validate:"required"`
	ConfigId    ConfigId               `validate:"required"`
	Content     *orderedmap.OrderedMap `validate:"dive"`
}

func (k PhaseKey) Kind() Kind {
	return Kind{Name: PhaseKind, Abbr: PhaseAbbr}
}

func (k TaskKey) Kind() Kind {
	return Kind{Name: TaskKind, Abbr: TaskAbbr}
}

func (k PhaseKey) ObjectId() string {
	return cast.ToString(k.Index)
}

func (k TaskKey) ObjectId() string {
	return cast.ToString(k.Index)
}

func (k PhaseKey) Level() ObjectLevel {
	return 4
}

func (k TaskKey) Level() ObjectLevel {
	return 5
}

func (k PhaseKey) Key() Key {
	return k
}

func (k TaskKey) Key() Key {
	return k
}

func (k TaskKey) String() string {
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/phase:%d/task:%d"`, k.Kind().Name, k.BranchId, k.ComponentId, k.ConfigId, k.PhaseKey.Index, k.Index)
}

func (k PhaseKey) String() string {
	return fmt.Sprintf(`%s "branch:%d/component:%s/config:%s/phase:%d"`, k.Kind().Name, k.BranchId, k.ComponentId, k.ConfigId, k.Index)
}

func (k PhaseKey) ConfigKey() ConfigKey {
	return ConfigKey{
		BranchId:    k.BranchId,
		ComponentId: k.ComponentId,
		Id:          k.ConfigId,
	}
}

func (k PhaseKey) ParentKey() (Key, error) {
	return k.ConfigKey(), nil
}

func (k TaskKey) ConfigKey() ConfigKey {
	return k.PhaseKey.ConfigKey()
}

func (k TaskKey) ParentKey() (Key, error) {
	return k.PhaseKey, nil
}

func (t *Task) TargetConfigKey() ConfigKey {
	return ConfigKey{
		BranchId:    t.BranchId,
		ComponentId: t.ComponentId,
		Id:          t.ConfigId,
	}
}

// UsedInOrchestratorRelation indicates that the owner config is used in an orchestration.
type UsedInOrchestratorRelation struct {
	ConfigId ConfigId
}

func (t *UsedInOrchestratorRelation) Type() RelationType {
	return UsedInOrchestratorRelType
}

func (t *UsedInOrchestratorRelation) String() string {
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

func (t *UsedInOrchestratorRelation) NewOtherSideRelation(_ Object, _ Objects) (Key, Relation, error) {
	return nil, nil, nil
}
