package model

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
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
	ConfigPath    string                 // target config path if any
	Content       *orderedmap.OrderedMap `validate:"dive"`
}

func (v *Orchestration) Clone() *Orchestration {
	if v == nil {
		return nil
	}
	clone := *v
	clone.Phases = make([]*Phase, 0)
	for _, phase := range v.Phases {
		clone.Phases = append(clone.Phases, phase.Clone())
	}
	return &clone
}

func (p *Phase) Clone() *Phase {
	if p == nil {
		return nil
	}
	clone := *p
	clone.Content = p.Content.Clone()
	clone.Tasks = make([]*Task, 0)
	for _, task := range p.Tasks {
		clone.Tasks = append(clone.Tasks, task.Clone())
	}
	return &clone
}

func (t *Task) Clone() *Task {
	if t == nil {
		return nil
	}
	clone := *t
	clone.Content = t.Content.Clone()
	return &clone
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
