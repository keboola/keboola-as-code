package model

import (
	"fmt"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

var (
	OrchestrationKind = Kind{Name: "orchestration", Abbr: "o", ToMany: false}
	PhaseKind         = Kind{Name: "phase", Abbr: "p", ToMany: true}
	TaskKind          = Kind{Name: "task", Abbr: "t", ToMany: true}
)

type OrchestrationKey struct {
	ConfigKey `validate:"dive" `
}

type PhaseKey struct {
	OrchestrationKey `validate:"dive" `
	PhaseIndex       int `validate:"min=0" `
}

type TaskKey struct {
	PhaseKey  `validate:"dive" `
	TaskIndex int `validate:"min=0" `
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

func (k Kind) IsOrchestration() bool {
	return k == OrchestrationKind
}

func (k Kind) IsPhase() bool {
	return k == PhaseKind
}

func (k Kind) IsTask() bool {
	return k == TaskKind
}

func (k OrchestrationKey) Kind() Kind {
	return OrchestrationKind
}

func (k OrchestrationKey) Level() ObjectLevel {
	return 45
}

func (k OrchestrationKey) Key() Key {
	return k
}

func (k OrchestrationKey) ParentKey() (Key, error) {
	return k.ConfigKey, nil
}

func (k OrchestrationKey) String() string {
	return fmt.Sprintf(`%s "%s"`, k.Kind().Name, k.LogicPath())
}

func (k OrchestrationKey) LogicPath() string {
	return k.ConfigKey.LogicPath() + "/orchestration"
}

func (k OrchestrationKey) ObjectId() string {
	return "orchestration"
}

func (k PhaseKey) Kind() Kind {
	return PhaseKind
}

func (k PhaseKey) ObjectId() string {
	return cast.ToString(k.PhaseIndex)
}

func (k PhaseKey) Level() ObjectLevel {
	return 46
}

func (k PhaseKey) Key() Key {
	return k
}

func (k PhaseKey) String() string {
	return fmt.Sprintf(`%s "%s"`, k.Kind().Name, k.LogicPath())
}

func (k PhaseKey) LogicPath() string {
	return k.OrchestrationKey.LogicPath() + fmt.Sprintf("/phase:%03d", k.PhaseIndex+1)
}

func (k PhaseKey) ParentKey() (Key, error) {
	return k.OrchestrationKey, nil
}

func (p *Phase) ObjectName() string {
	return p.Name
}

func (k TaskKey) Kind() Kind {
	return TaskKind
}

func (k TaskKey) ObjectId() string {
	return cast.ToString(k.PhaseIndex)
}

func (k TaskKey) Level() ObjectLevel {
	return 47
}

func (k TaskKey) Key() Key {
	return k
}

func (k TaskKey) String() string {
	return fmt.Sprintf(`%s "%s"`, k.Kind().Name, k.LogicPath())
}

func (k TaskKey) LogicPath() string {
	return k.PhaseKey.LogicPath() + fmt.Sprintf("/task:%03d", k.TaskIndex+1)
}

func (k TaskKey) ParentKey() (Key, error) {
	return k.PhaseKey, nil
}

func (t *Task) ObjectName() string {
	return t.Name
}

func (t *Task) TargetConfigKey() ConfigKey {
	return ConfigKey{
		BranchKey:   BranchKey{BranchId: t.BranchId},
		ComponentId: t.ComponentId,
		ConfigId:    t.ConfigId,
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
