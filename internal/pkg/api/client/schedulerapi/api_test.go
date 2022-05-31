package schedulerapi_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/api/client/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/http"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestSchedulerApiCalls(t *testing.T) {
	t.Parallel()

	project := testproject.GetTestProject(t, env.Empty())
	project.SetState("empty.json")
	storageApi := project.StorageApi()
	services, err := storageApi.ServicesUrlById()
	assert.NoError(t, err)
	baseUrl, found := services[`scheduler`]
	assert.True(t, found)
	client := http.New(
		context.Background(),
		http.WithBaseUrl(baseUrl.String()),
		http.WithHeader("X-StorageApi-Token", storageApi.Token().Token),
	)

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
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "foo",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
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
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "schedule",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "cronTab", Value: "*/2 * * * *"},
						{Key: "timezone", Value: "UTC"},
						{Key: "state", Value: "disabled"},
					}),
				},
				{
					Key: "target",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
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

	// List should return no schedule
	_, schedules, err := ListSchedulesRequest().Send(client)
	assert.NoError(t, err)
	assert.Len(t, schedules, 0)

	// Activate
	_, schedule, err := ActivateScheduleRequest(resConfigScheduler.Id, "").Send(client)
	assert.NoError(t, err)
	assert.NotNil(t, schedule)
	assert.NotEmpty(t, schedule.Id)

	// List should return one schedule
	_, schedules, err = ListSchedulesRequest().Send(client)
	assert.NoError(t, err)
	assert.Len(t, schedules, 1)

	// Delete
	_, _, err = DeleteScheduleRequest(schedule.Id).Send(client)
	assert.NoError(t, err)

	// List should return no schedule
	_, schedules, err = ListSchedulesRequest().Send(client)
	assert.NoError(t, err)
	assert.Len(t, schedules, 0)

	// Activate again
	_, schedule, err = ActivateScheduleRequest(resConfigScheduler.Id, "").Send(client)
	assert.NoError(t, err)
	assert.NotNil(t, schedule)
	assert.NotEmpty(t, schedule.Id)

	// List should return one schedule
	_, schedules, err = ListSchedulesRequest().Send(client)
	assert.NoError(t, err)
	assert.Len(t, schedules, 1)

	// Delete for configuration
	_, _, err = DeleteSchedulesForConfigurationRequest(resConfigScheduler.Id).Send(client)
	assert.NoError(t, err)

	// List should return no schedule
	_, schedules, err = ListSchedulesRequest().Send(client)
	assert.NoError(t, err)
	assert.Len(t, schedules, 0)
}
