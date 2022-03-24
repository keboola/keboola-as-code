package model

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
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
	AbsPath
	DependsOn []PhaseKey
	Tasks     []*Task                `validate:"dive"`
	Name      string                 `validate:"required"`
	Content   *orderedmap.OrderedMap `validate:"required"`
}

type Task struct {
	TaskKey
	AbsPath     `validate:"dive"`
	Name        string                 `validate:"required"`
	ComponentId ComponentId            `validate:"required"`
	ConfigId    ConfigId               `validate:"required"`
	ConfigPath  string                 // target config path if any
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

func (k PhaseKey) Level() int {
	return 4
}

func (k TaskKey) Level() int {
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

func (p Phase) String() string {
	buf := new(bytes.Buffer)
	_, _ = fmt.Fprintf(buf, "#  %03d %s\n", p.Index+1, p.Name)

	var dependsOn []string
	for _, dependsOnKey := range p.DependsOn {
		dependsOn = append(dependsOn, cast.ToString(dependsOnKey.Index+1))
	}

	_, _ = fmt.Fprintf(buf, "depends on phases: [%s]\n", strings.Join(dependsOn, `, `))
	_, _ = fmt.Fprintln(buf, json.MustEncodeString(p.Content, true))
	for _, task := range p.Tasks {
		_, _ = fmt.Fprint(buf, task.String())
	}
	return strings.TrimRight(buf.String(), "\n")
}

func (t Task) String() string {
	targetConfigDesc := t.ConfigPath
	if len(targetConfigDesc) == 0 {
		targetConfigDesc = fmt.Sprintf(`branch:%d/componentId:%s/configId:%s`, t.BranchId, t.ComponentId, t.ConfigId)
	}
	return fmt.Sprintf(
		"## %03d %s\n>> %s\n%s",
		t.Index+1,
		t.Name,
		targetConfigDesc,
		json.MustEncodeString(t.Content, true),
	)
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
