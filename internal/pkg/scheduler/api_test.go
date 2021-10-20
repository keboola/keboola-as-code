package scheduler_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
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
				BranchId:    branch.Id,
				ComponentId: "keboola.scheduler",
			},
			Name:              "Test",
			Description:       "Test description",
			ChangeDescription: "My test",
			Content: utils.PairsToOrderedMap([]utils.Pair{
				{
					Key: "schedule",
					Value: utils.PairsToOrderedMap([]utils.Pair{
						{Key: "cronTab", Value: "*/2 * * * *"},
						{Key: "timezone", Value: "UTC"},
						{Key: "state", Value: "disabled"},
					}),
				},
				{
					Key: "target",
					Value: utils.PairsToOrderedMap([]utils.Pair{
						{Key: "componentId", Value: "ex-generic-v2"},
						{Key: "configurationId", Value: configTarget.Id},
						{Key: "mode", Value: "run"},
					}),
				},
			}),
		},
	}
	resConfigScheduler, err := storageApi.CreateConfig(configScheduler)
	assert.NoError(t, err)

	api := scheduler.NewSchedulerApi(hostName, token, context.Background(), logger, true)

	// List
	schedules, err := api.ListSchedules()
	assert.NoError(t, err)
	schedulesLength := len(schedules)

	// Activate
	schedule, err := api.ActivateSchedule(resConfigScheduler.Id, "")
	assert.NoError(t, err)
	assert.NotNil(t, schedule)
	assert.NotEmpty(t, schedule.Id)

	// List should return one more schedule
	schedules, err = api.ListSchedules()
	assert.NoError(t, err)
	assert.Len(t, schedules, schedulesLength+1)

	// Delete
	deleteResponseErr := api.DeleteSchedule(schedule.Id)
	assert.NoError(t, deleteResponseErr)

	// List should return one less schedule
	schedules, err = api.ListSchedules()
	assert.NoError(t, err)
	assert.Len(t, schedules, schedulesLength)

	// Activate again
	schedule, err = api.ActivateSchedule(resConfigScheduler.Id, "")
	assert.NoError(t, err)
	assert.NotNil(t, schedule)
	assert.NotEmpty(t, schedule.Id)

	// List should return one more schedule
	schedules, err = api.ListSchedules()
	assert.NoError(t, err)
	assert.Len(t, schedules, schedulesLength+1)

	// Delete for configuration
	deleteResponseErr = api.DeleteSchedulesForConfiguration(resConfigScheduler.Id)
	assert.NoError(t, deleteResponseErr)

	// List should return one less schedule
	schedules, err = api.ListSchedules()
	assert.NoError(t, err)
	assert.Len(t, schedules, schedulesLength)
}
