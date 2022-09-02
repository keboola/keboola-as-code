package scheduler_test

import (
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSchedulerMapperRemoteActivate(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Branch
	branchKey := model.BranchKey{
		Id: 123,
	}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{
			BranchKey: branchKey,
		},
		Remote: &model.Branch{
			BranchKey: branchKey,
			IsDefault: true,
		},
	}
	assert.NoError(t, state.Set(branchState))

	// Scheduler config
	schedulerKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: storageapi.SchedulerComponentID,
		Id:          `456`,
	}
	schedulerConfigState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: schedulerKey,
		},
		Remote: &model.Config{
			ConfigKey: schedulerKey,
		},
	}
	assert.NoError(t, state.Set(schedulerConfigState))

	// Expected HTTP call
	var httpRequest *http.Request
	d.MockedHttpTransport().RegisterResponder(resty.MethodPost, `=~schedules`,
		func(req *http.Request) (*http.Response, error) {
			httpRequest = req
			return httpmock.NewStringResponse(200, `{"id": "789"}`), nil
		},
	)

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddUpdated(schedulerConfigState)
	assert.NoError(t, state.Mapper().AfterRemoteOperation(context.Background(), changes))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Check API request
	reqBody, err := io.ReadAll(httpRequest.Body)
	assert.NoError(t, err)
	assert.NoError(t, err)
	assert.Equal(t, string(reqBody), `{"configurationId":"456"}`)
}
