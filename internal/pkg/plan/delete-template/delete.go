package delete_template

import (
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/search"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

// NewPlan creates a plan for renaming objects that do not match the naming.
func NewPlan(projectState *state.State, branchKey model.BranchKey, instanceId string) (*Plan, error) {
	builder := &planBuilder{State: projectState}
	actions, err := builder.build(branchKey, instanceId)
	if err != nil {
		return nil, err
	}
	return &Plan{actions: actions, projectState: projectState, branchKey: branchKey, instanceId: instanceId}, nil
}

type planBuilder struct {
	*state.State
	actions []DeleteAction
}

func (b *planBuilder) build(branchKey model.BranchKey, instanceId string) ([]DeleteAction, error) {
	configsMap := map[storageapi.ConfigID]bool{}
	for _, config := range search.ConfigsForTemplateInstance(b.State.LocalObjects().ConfigsWithRowsFrom(branchKey), instanceId) {
		configState := b.MustGet(config.Key())
		action := DeleteAction{
			State:    configState,
			Manifest: configState.Manifest(),
		}
		b.actions = append(b.actions, action)

		configsMap[config.Id] = true
	}

	// Search for schedules and delete those belonging to the deleted configs
	for _, config := range b.ConfigsFrom(branchKey) {
		component, err := b.Components().GetOrErr(config.ComponentId)
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
				if configsMap[schedulerRel.ConfigId] {
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
