package delete_template

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

// NewPlan creates a plan for renaming objects that do not match the naming.
func NewPlan(projectState *state.State, branchKey model.BranchKey, instanceID string) (*Plan, error) {
	builder := &planBuilder{State: projectState}
	actions, err := builder.build(branchKey, instanceID)
	if err != nil {
		return nil, err
	}
	return &Plan{actions: actions, projectState: projectState, branchKey: branchKey, instanceID: instanceID}, nil
}

type planBuilder struct {
	*state.State
	actions []DeleteAction
}

func (b *planBuilder) build(branchKey model.BranchKey, instanceID string) ([]DeleteAction, error) {
	configsMap := map[keboola.ConfigID]bool{}
	for _, config := range search.ConfigsForTemplateInstance(b.State.LocalObjects().ConfigsWithRowsFrom(branchKey), instanceID) {
		configState := b.MustGet(config.Key())
		action := DeleteAction{
			State:    configState,
			Manifest: configState.Manifest(),
		}
		b.actions = append(b.actions, action)

		configsMap[config.ID] = true
	}

	// Search for schedules and delete those belonging to the deleted configs
	for _, config := range b.ConfigsFrom(branchKey) {
		component, err := b.Components().GetOrErr(config.ComponentID)
		if err != nil {
			return nil, err
		}

		if component.IsScheduler() {
			rel, err := config.Relations.GetOneByType(model.SchedulerForRelType)
			if err != nil {
				return nil, err
			}
			if rel != nil {
				schedulerRel := rel.(*model.SchedulerForRelation)
				if configsMap[schedulerRel.ConfigID] {
					action := DeleteAction{
						State:    config.NewObjectState(),
						Manifest: config.Manifest(),
					}
					b.actions = append(b.actions, action)
				}
			}
		}
	}

	return b.actions, nil
}
