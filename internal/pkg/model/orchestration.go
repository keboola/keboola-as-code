package model

import (
	"fmt"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

const (
	OrchestrationKind = "orchestration"
	PhaseKind         = "phase"
	TaskKind          = "task"
	OrchestrationAbbr = "o"
	PhaseAbbr         = "p"
	TaskAbbr          = "t"
)

type OrchestrationKey struct {
	Parent ConfigKey `json:"-" validate:"dive" `
}

type PhaseKey struct {
	Parent OrchestrationKey `json:"-" validate:"dive" `
	Index  int              `json:"-" validate:"min=0" `
}

type TaskKey struct {
	Parent PhaseKey `json:"-" validate:"dive" `
	Index  int      `json:"-" validate:"min=0" `
}

type Orchestration struct {
	OrchestrationKey `validate:"dive"`
}

type Phase struct {
	PhaseKey  `validate:"dive"`
	DependsOn []PhaseKey
	Name      string                 `validate:"required" diff:"true"`
	Content   *orderedmap.OrderedMap `validate:"required" diff:"true"`
}

type Task struct {
	TaskKey     `validate:"dive" `
	Name        string                 `validate:"required"`
	ComponentId ComponentId            `validate:"required"`
	ConfigId    ConfigId               `validate:"required"`
	Content     *orderedmap.OrderedMap `validate:"dive"`
}

func (k OrchestrationKey) Kind() Kind {
	return Kind{Name: OrchestrationKind, Abbr: OrchestrationAbbr}
}

func (k OrchestrationKey) Level() ObjectLevel {
	return 21
}

func (k OrchestrationKey) Key() Key {
	return k
}

func (k OrchestrationKey) ParentKey() (Key, error) {
	return k.Parent, nil
}

func (k OrchestrationKey) String() string {
	return fmt.Sprintf(`%s "%s"`, k.Kind().Name, k.LogicPath())
}

func (k OrchestrationKey) LogicPath() string {
	return k.Parent.LogicPath() + "orchestration"
}

func (k OrchestrationKey) ObjectId() string {
	return "orchestration"
}

func (k PhaseKey) Kind() Kind {
	return Kind{Name: PhaseKind, Abbr: PhaseAbbr}
}

func (k PhaseKey) ObjectId() string {
	return cast.ToString(k.Index)
}

func (k PhaseKey) Level() ObjectLevel {
	return 22
}

func (k PhaseKey) Key() Key {
	return k
}

func (k PhaseKey) String() string {
	return fmt.Sprintf(`%s "%s"`, k.Kind().Name, k.LogicPath())
}

func (k PhaseKey) LogicPath() string {
	return k.Parent.LogicPath() + fmt.Sprintf("/phase:%d", k.Index)
}

func (k PhaseKey) ParentKey() (Key, error) {
	return k.Parent, nil
}

func (p *Phase) ObjectName() string {
	return p.Name
}

func (k TaskKey) Kind() Kind {
	return Kind{Name: TaskKind, Abbr: TaskAbbr}
}

func (k TaskKey) ObjectId() string {
	return cast.ToString(k.Index)
}

func (k TaskKey) Level() ObjectLevel {
	return 23
}

func (k TaskKey) Key() Key {
	return k
}

func (k TaskKey) String() string {
	return fmt.Sprintf(`%s "%s"`, k.Kind().Name, k.LogicPath())
}

func (k TaskKey) LogicPath() string {
	return k.Parent.LogicPath() + fmt.Sprintf("/task:%d", k.Index)
}

func (k TaskKey) ParentKey() (Key, error) {
	return k.Parent, nil
}

func (t *Task) ObjectName() string {
	return t.Name
}

func (t *Task) TargetConfigKey() ConfigKey {
	return ConfigKey{
		BranchId:    t.TaskKey.Parent.Parent.Parent.BranchId,
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
