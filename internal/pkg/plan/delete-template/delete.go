package delete_template

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

// NewPlan creates a plan for renaming objects that do not match the naming.
func NewPlan(projectState *state.State, branch model.BranchKey, instance string) (*Plan, error) {
	builder := &planBuilder{State: projectState}
	actions := builder.build(branch, instance)
	return &Plan{actions: actions}, nil
}

type planBuilder struct {
	*state.State
	actions []DeleteAction
}

func (b *planBuilder) build(branch model.BranchKey, instance string) []DeleteAction {
	for _, config := range search.ConfigsForTemplateInstance(b.State.LocalObjects().ConfigsWithRowsFrom(branch), instance) {
		configState, _ := b.Get(config.Key())
		action := DeleteAction{
			State:    configState,
			Manifest: configState.Manifest(),
		}
		b.actions = append(b.actions, action)
	}

	return b.actions
}
