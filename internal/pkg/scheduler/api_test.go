package scheduler_test

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestSchedulerApiCalls(t *testing.T) {
	t.Parallel()
	logger, _ := utils.NewDebugLogger()

	project := testproject.GetTestProject(t, env.Empty())
	project.SetState("empty.json")
	storageApi := project.Api()
	token := storageApi.Token().Token
	hostName, _ := storageApi.GetSchedulerApiUrl()

	// Get default branch
	branch, err := storageApi.GetDefaultBranch()
	assert.NoError(t, err)
	assert.NotNil(t, branch)

	configTarget := &model.ConfigWithRows{
		Config: &model.Config{
			ConfigKey: model.ConfigKey{
				BranchId:    branch.Id,
				ComponentId: "ex-generic-v2",
			},
			Name:              "Test",
			Description:       "Test description",
			ChangeDescription: "My test",
			Content: utils.PairsToOrderedMap([]utils.Pair{
				{
					Key: "foo",
					Value: utils.PairsToOrderedMap([]utils.Pair{
						{Key: "bar", Value: "baz"},
					}),
				},
			}),
		},
	}
	resConfigTarget, err := storageApi.CreateConfig(configTarget)
	assert.NoError(t, err)
	assert.Same(t, configTarget, resConfigTarget)
	assert.NotEmpty(t, configTarget.Id)

	configScheduler := &model.ConfigWithRows{
		Config: &model.Config{
			ConfigKey: model.ConfigKey{
				BranchId: branch.Id,
				ComponentId: "keboola.scheduler",
			},
			Name: "Test",
			Description: "Test description",
			ChangeDescription: "My test",
			Content: utils.PairsToOrderedMap([]utils.Pair{
				{
					Key: "schedule",
					Value: utils.PairsToOrderedMap([]utils.Pair{
						{ Key: "cronTab", Value: "*/2 * * * *" },
						{ Key: "timezone", Value: "UTC" },
						{ Key: "state", Value: "disabled" },
					}),
				},
				{
					Key: "target",
					Value: utils.PairsToOrderedMap([]utils.Pair{
						{ Key: "componentId", Value: "ex-generic-v2" },
						{ Key: "configurationId", Value: configTarget.Id},
						{ Key: "mode", Value: "run" },
					}),
				},
			}),
		},
	}
	resConfigScheduler, err := storageApi.CreateConfig(configScheduler)
	assert.NoError(t, err)

	api := scheduler.NewSchedulerApi(hostName, token, context.Background(), logger, true)

	// Activate
	schedule, err := api.ActivateSchedule(resConfigScheduler.Id, "")
	assert.NoError(t, err)
	assert.NotNil(t, schedule)
	assert.NotEmpty(t, schedule.Id)

	// Delete
	deleteResponseErr := api.DeleteSchedule(schedule.Id)
	assert.NoError(t, deleteResponseErr)
}
