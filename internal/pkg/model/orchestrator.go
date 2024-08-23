package model

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
)

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

// Task is a part of an orchestration Phase.
// Configuration can be defined by ConfigId or ConfigData.
// ConfigId and ConfigData can be empty if Enabled=false.
type Task struct {
	TaskKey
	AbsPath
	Name        string `validate:"required"`
	Enabled     bool
	ComponentID keboola.ComponentID `validate:"required"`
	ConfigID    keboola.ConfigID
	ConfigData  *orderedmap.OrderedMap
	ConfigPath  string // target config path if any
	Content     *orderedmap.OrderedMap
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
		targetConfigDesc = fmt.Sprintf(`branch:%d/componentId:%s/configId:%s`, t.BranchID, t.ComponentID, t.ConfigID)
	}

	// Add field enabled to task.Content
	content := t.Content.Clone()
	content.Set("enabled", t.Enabled)

	return fmt.Sprintf(
		"## %03d %s\n>> %s\n%s",
		t.Index+1,
		t.Name,
		targetConfigDesc,
		json.MustEncodeString(content, true),
	)
}

// UsedInOrchestratorRelation indicates that the owner config is used in an orchestration.
type UsedInOrchestratorRelation struct {
	ConfigID keboola.ConfigID
}

func (t *UsedInOrchestratorRelation) Type() RelationType {
	return UsedInOrchestratorRelType
}

func (t *UsedInOrchestratorRelation) Desc() string {
	return fmt.Sprintf(`used in orchestrator "%s"`, t.ConfigID)
}

func (t *UsedInOrchestratorRelation) Key() string {
	return fmt.Sprintf(`%s_%s`, t.Type(), t.ConfigID)
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

func (t *UsedInOrchestratorRelation) IsDefinedInAPI() bool {
	return false
}

func (t *UsedInOrchestratorRelation) NewOtherSideRelation(_ Object, _ Objects) (Key, Relation, error) {
	return nil, nil, nil
}
